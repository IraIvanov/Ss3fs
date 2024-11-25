package ss3fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/winfsp/cgofuse/fuse"
)

var (
	ErrMountPointDoesntExist = errors.New("mounting bucket doesn't exist")
)

type Ss3fs struct {
	clnt   *s3.Client
	ctx    *context.Context
	bucket string /* aws string */
	opened map[string]*Attrs
	fuse.FileSystemBase
	lock     sync.RWMutex
	rootAttr fuse.Stat_t
}

type Attrs struct {
	stat   fuse.Stat_t
	refCnt uint64
}

func NewSs3fs(AccKey *string, SecKey *string, Region *string, Bucket *string, EndPoint *string) (*Ss3fs, error) {
	/* add cred and EP validation*/
	os.Setenv("AWS_ACCESS_KEY", *AccKey)
	os.Setenv("AWS_SECRET_KEY", *SecKey)
	os.Setenv("AWS_DEFAULT_REGION", *Region)
	ctx := context.Background()
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Printf("Can't initialize sdk config, error is %v\n", err)
		return nil, err
	}
	sdkConfig.BaseEndpoint = EndPoint
	fs := Ss3fs{}
	fs.clnt = s3.NewFromConfig(sdkConfig)
	fs.ctx = &ctx
	fs.bucket = *Bucket
	fs.opened = make(map[string]*Attrs)
	fs.rootAttr = fuse.Stat_t{
		Atim:  fuse.Now(),
		Ctim:  fuse.Now(),
		Mtim:  fuse.Now(),
		Nlink: 1,
		Gid:   uint32(os.Getgid()),
		Uid:   uint32(os.Getuid()),
		Mode:  fuse.S_IFDIR | 0555,
	}
	/* check if bucket exists */
	exists, _ := BucketExists(fs.bucket, fs.clnt, fs.ctx)
	if !exists {
		return nil, ErrMountPointDoesntExist
	}
	return &fs, nil
}

func (fs *Ss3fs) Readdir(path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errc int) {
	fs.lock.RLock()
	defer fs.lock.RUnlock()
	switch path {
	case "/":
		fill(".", nil, 0)
		fill("..", nil, 0)
		/* list objects in specified bucket */
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(fs.bucket),
		}
		result, err := fs.clnt.ListObjectsV2(*fs.ctx, input)
		if err != nil {
			log.Printf("List objects failed with error %v\n", err)
			return -fuse.EIO
		}
		for _, object := range result.Contents {
			fill(*object.Key, nil, 0)
		}
	default:
		/* add listing of directory objects */
		return -fuse.ENOENT
	}
	return 0
}

func (fs *Ss3fs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	fs.lock.RLock()
	defer fs.lock.RUnlock()
	switch path {
	case "/":
		*stat = fs.rootAttr
		stat.Atim = fuse.Now()
		return 0
	default:
		/* erase '/' */
		name := path[1:]
		var attr Attrs
		exists, err := ObjectExist(fs.bucket, name, fs.clnt, fs.ctx, &attr)
		if exists {
			stat.Uid = uint32(os.Getuid())
			stat.Gid = uint32(os.Getgid())
			/* check for permitions */
			stat.Mode = fuse.S_IFREG | 0666
			stat.Mtim = attr.stat.Mtim
			stat.Ctim = attr.stat.Ctim
			stat.Size = attr.stat.Size
			stat.Atim = fuse.Now()
			stat.Nlink = 1

		} else {
			return -fuse.ENOENT
		}
		if err != nil {
			log.Printf("Head object failed with error %v\n", err)
			return -fuse.EIO
		}
	}
	return 0
}

func (fs *Ss3fs) Read(path string, buff []byte, ofst int64, fh uint64) (n int) {
	fs.lock.RLock()
	defer fs.lock.RUnlock()
	endofst := ofst + int64(len(buff))
	name := path[1:]
	attr, ok := fs.opened[name]
	if !ok {
		attr = &Attrs{}
	}
	attr.stat.Atim = fuse.Now()

	exists, err := ObjectExist(fs.bucket, name, fs.clnt, fs.ctx, attr)
	if endofst > attr.stat.Size {
		endofst = attr.stat.Size
	}
	if exists {
		objRange := fmt.Sprintf("%d-%d", ofst, endofst)
		input := &s3.GetObjectInput{
			Bucket: aws.String(fs.bucket),
			Key:    aws.String(name),
			Range:  aws.String(objRange),
		}
		result, err := fs.clnt.GetObject(*fs.ctx, input)
		if err != nil {
			log.Printf("Get object failed with error %v\n", err)
			return -fuse.EIO
		}
		defer result.Body.Close()
		body, err := io.ReadAll(result.Body)
		if err != nil {
			log.Printf("Read content failed with error %v\n", err)
			return -fuse.EIO
		}
		n = copy(buff, body)
		return
	} else {
		if err != nil {
			log.Printf("Head object faild with error %v\n", err)
			return -fuse.EIO
		}
		return -fuse.ENOENT
	}
}

func (fs *Ss3fs) Write(path string, buff []byte, ofst int64, fh uint64) (n int) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	name := path[1:]
	attr, ok := fs.opened[name]
	if !ok {
		attr = &Attrs{}
	}
	attr.stat.Atim = fuse.Now()
	attr.stat.Mtim = fuse.Now()

	exists, err := ObjectExist(fs.bucket, name, fs.clnt, fs.ctx, attr)
	if !exists {
		return -fuse.ENOENT
	}
	if err != nil {
		log.Printf("Head object failed with error %v\n", err)
		return -fuse.EIO
	}
	file, err := os.Create("/tmp/" + name)
	if err != nil {
		log.Printf("Create tmp file failed with error %v\n", err)
		return -fuse.EIO
	}
	defer os.Remove(file.Name())

	var partSize, readOfst, endofst int64 = 10 * 1024 * 1024, 0, 0
	/* download file in 10Mb chunks */
	for endofst < attr.stat.Size {
		objRange := fmt.Sprintf("%d-%d", readOfst, endofst)
		input := &s3.GetObjectInput{
			Bucket: aws.String(fs.bucket),
			Key:    aws.String(name),
			Range:  aws.String(objRange),
		}
		result, err := fs.clnt.GetObject(*fs.ctx, input)
		if err != nil {
			log.Printf("Get object failed with error %v\n", err)
			return -fuse.EIO
		}
		defer result.Body.Close()
		body, err := io.ReadAll(result.Body)
		if err != nil {
			log.Printf("Read content failed with error %v\n", err)
			return -fuse.EIO
		}
		_, ok := file.WriteAt(body, readOfst)
		if ok != nil {
			log.Printf("Write into tmp file failed with error %v\n", err)
			return -fuse.EIO
		}
		readOfst = endofst
		endofst += partSize
		if endofst > attr.stat.Size {
			endofst = attr.stat.Size
		}
	}
	n, err = file.WriteAt(buff, ofst)
	if err != nil {
		log.Printf("Write into tmp file failed with error %v\n", err)
		return -fuse.EIO
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
		Body:   file,
	}
	_, err = fs.clnt.PutObject(*fs.ctx, input)
	if err != nil {
		log.Printf("Put object failed with error %v\n", err)
		return -fuse.EIO
	}
	attr.stat.Size = ofst + int64(n)
	return
}

func (fs *Ss3fs) Mknod(path string, mode uint32, dev uint64) (errc int) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	name := path[1:]
	_, ok := fs.opened[name]
	if ok {
		return -fuse.EEXIST
	}
	exists, err := ObjectExist(fs.bucket, name, fs.clnt, fs.ctx, nil)
	if exists {
		return -fuse.EEXIST
	}
	if err != nil {
		log.Printf("Head object failed with error %v\n", err)
		return -fuse.EIO

	}
	input := &s3.PutObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	}
	_, err = fs.clnt.PutObject(*fs.ctx, input)
	if err != nil {
		log.Printf("Put object failed with error %v\n", err)
		return 0
	}
	return 0
}

func (fs *Ss3fs) Utimens(path string, tmsp []fuse.Timespec) (errc int) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	name := path[1:]
	attr := fs.opened[path]
	exists, err := ObjectExist(fs.bucket, name, fs.clnt, fs.ctx, attr)
	if !exists {
		return -fuse.ENOENT
	}
	if err != nil {
		log.Printf("Head object failed with error %v\n", err)
		return -fuse.EIO
	}
	if attr != nil {
		attr.stat.Atim = fuse.Now()
		attr.stat.Ctim = fuse.Now()
		attr.stat.Mtim = fuse.Now()
	}
	if tmsp == nil {
		tmsp0 := fuse.Now()
		tmsa := [2]fuse.Timespec{tmsp0, tmsp0}
		tmsp = tmsa[:]
	}
	return 0
}

func (fs *Ss3fs) Open(path string, flags int) (errc int, fh uint64) {
	fs.lock.RLock()
	defer fs.lock.RUnlock()
	name := path[1:]
	attr := fs.opened[name]

	if attr == nil {
		attr = &Attrs{}
		exists, err := ObjectExist(fs.bucket, name, fs.clnt, fs.ctx, attr)
		if !exists {
			if err != nil {
				log.Printf("Head object failed with error %v\n", err)
				return -fuse.EIO, ^uint64(0)
			}
			return -fuse.ENOENT, ^uint64(0)
		}
		fs.opened[name] = attr
	}
	attr.stat.Atim = fuse.Now()
	attr.refCnt += 1
	return 0, 0
}

func (fs *Ss3fs) Release(path string, fh uint64) (errc int) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	name := path[1:]
	attr, ok := fs.opened[name]
	if ok {
		attr.refCnt--
		if attr.refCnt <= 0 {
			delete(fs.opened, name)
		}
		return 0
	}
	return -fuse.EBADF
}

func (fs *Ss3fs) Unlink(path string) (errc int) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	name := path[:]
	_, ok := fs.opened[name]
	if ok {
		delete(fs.opened, name)
	}
	exists, err := ObjectExist(fs.bucket, name, fs.clnt, fs.ctx, nil)
	if !exists {
		if err != nil {
			log.Printf("Head object failed with error %v\n", err)
			return -fuse.EIO
		}
		return -fuse.ENOENT
	}
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	}
	_, err = fs.clnt.DeleteObject(*fs.ctx, input)
	if err != nil {
		log.Printf("Delte object failed with error %v\n", err)
		return -fuse.EIO
	}
	return 0
}

func (fs *Ss3fs) Rename(oldpath string, newpath string) int {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	name := oldpath[1:]
	newName := newpath[1:]
	_, ok := fs.opened[name]
	if ok {
		delete(fs.opened, name)
		//return -fuse.EBUSY
	}
	exists, err := ObjectExist(fs.bucket, name, fs.clnt, fs.ctx, nil)
	if !exists {
		if err != nil {
			log.Printf("Head object failed with error %v\n", err)
			return -fuse.EIO
		}
		return -fuse.ENOENT
	}
	exists, _ = ObjectExist(fs.bucket, newName, fs.clnt, fs.ctx, nil)
	if exists {
		return -fuse.EEXIST
	}
	copyInput := &s3.CopyObjectInput{
		Bucket:     aws.String(fs.bucket),
		Key:        aws.String(newName),
		CopySource: aws.String(fs.bucket + oldpath),
	}
	_, err = fs.clnt.CopyObject(*fs.ctx, copyInput)
	if err != nil {
		log.Printf("Copy object failed with error %v\n", err)
		return -fuse.EIO
	}
	delInput := &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	}
	_, err = fs.clnt.DeleteObject(*fs.ctx, delInput)
	if err != nil {
		log.Printf("Delte object failed with error %v\n", err)
		return -fuse.EIO
	}
	return 0
}

// Left here for debug perposes
// Statfs gets file system statistics.
// The FileSystemBase implementation returns -ENOSYS.
/*
func (*Ss3fs) Statfs(path string, stat *fuse.Statfs_t) int {
	log.Println("called statfs")
	return -fuse.ENOSYS
}

// Mkdir creates a directory.
// The FileSystemBase implementation returns -ENOSYS.
func (*Ss3fs) Mkdir(path string, mode uint32) int {
	log.Println("called mkdir")
	return -fuse.ENOSYS
}

// Rmdir removes a directory.
// The FileSystemBase implementation returns -ENOSYS.
func (*Ss3fs) Rmdir(path string) int {
	log.Println("called rmdir")
	return -fuse.ENOSYS
}

// Link creates a hard link to a file.
// The FileSystemBase implementation returns -ENOSYS.
func (*Ss3fs) Link(oldpath string, newpath string) int {
	log.Println("called link")

	return -fuse.ENOSYS
}

// Symlink creates a symbolic link.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Symlink(target string, newpath string) int {
	log.Println("called symlink")

	return -fuse.ENOSYS
}

// Readlink reads the target of a symbolic link.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Readlink(path string) (int, string) {
	log.Println("called readlink")

	return -fuse.ENOSYS, ""
}

// Rename renames a file.
// The Ss3fs implementation returns -fuse.ENOSYS.

// Chmod changes the permission bits of a file.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Chmod(path string, mode uint32) int {
	log.Println("called chmod")
	return -fuse.ENOSYS
}

// Chown changes the owner and group of a file.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Chown(path string, uid uint32, gid uint32) int {
	log.Println("called chown")

	return -fuse.ENOSYS
}

// Access checks file access permissions.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Access(path string, mask uint32) int {
	log.Println("called access")

	return -fuse.ENOSYS
}

// Truncate changes the size of a file.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Truncate(path string, size int64, fh uint64) int {
	log.Println("called truncate")

	return -fuse.ENOSYS
}

// Flush flushes cached file data.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Flush(path string, fh uint64) int {
	log.Println("called flush")
	return -fuse.ENOSYS
}

// Fsync synchronizes file contents.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Fsync(path string, datasync bool, fh uint64) int {
	log.Println("called fsync")

	return -fuse.ENOSYS
}

// Opendir opens a directory.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Opendir(path string) (int, uint64) {
	log.Println("called opendir")

	return -fuse.ENOSYS, ^uint64(0)
}

// Releasedir closes an open directory.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Releasedir(path string, fh uint64) int {
	log.Println("called releasedir")

	return -fuse.ENOSYS
}

// Fsyncdir synchronizes directory contents.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Fsyncdir(path string, datasync bool, fh uint64) int {
	log.Println("called fsyncdir")

	return -fuse.ENOSYS
}

// Setxattr sets extended attributes.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Setxattr(path string, name string, value []byte, flags int) int {
	log.Println("called setxattr")

	return -fuse.ENOSYS
}

// Getxattr gets extended attributes.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Getxattr(path string, name string) (int, []byte) {
	log.Println("called getxxatr")

	return -fuse.ENOSYS, nil
}

// Removexattr removes extended attributes.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Removexattr(path string, name string) int {
	log.Println("called removexattr")

	return -fuse.ENOSYS
}

// Listxattr lists extended attributes.
// The Ss3fs implementation returns -fuse.ENOSYS.
func (*Ss3fs) Listxattr(path string, fill func(name string) bool) int {
	log.Println("called listxattr")

	return -fuse.ENOSYS
}
*/
