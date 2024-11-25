package ramfs

/* Package, that was used to test fuse Api */
/* It contains in memory fs, that allow user only to create, read and write files */

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/winfsp/cgofuse/fuse"
)

var (
	ErrMountPointDoesntExist = errors.New("mounting bucket doesn't exist")
)

type RamFs struct {
	created map[string]*Node
	opened  map[string]*Attrs
	ino     uint64
	ctime   fuse.Timespec
	lock    sync.Mutex
	fuse.FileSystemBase
}

type Node struct {
	attr Attrs
	ino  uint64
	data string
}

type Attrs struct {
	len   int64
	mtime fuse.Timespec
	ctime fuse.Timespec
	atime fuse.Timespec
	cnt   uint64
}

func (fs *RamFs) FsInit() error {
	var err error = nil
	fs.ino = 1
	fs.opened = make(map[string]*Attrs)
	fs.created = make(map[string]*Node)
	fs.ctime = fuse.NewTimespec(time.Now())
	/* check if bucket exists */
	return err
}

func (fs *RamFs) Readdir(path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errc int) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	/* add check for file type (object or bucket) */
	/* handle root directory in special way */
	log.Printf("reading dir\n")
	switch path {
	case "/":
		fill(".", nil, 0)
		fill("..", nil, 0)
		for name, node := range fs.created {
			log.Printf("%s %v\n", name, *node)
			fill(name[1:], nil, 0)
		}
	default:
		/* add listing of directory objects */
		return -fuse.ENOENT
	}
	return 0
}

func (fs *RamFs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	log.Printf("getting attrs of file %s\n", path)
	stat.Uid = uint32(os.Getuid())
	stat.Gid = uint32(os.Getgid())
	switch path {
	case "/":
		stat.Mode = fuse.S_IFDIR | 0555
		stat.Ctim = fs.ctime
		stat.Mtim = fuse.NewTimespec(time.Now())
		stat.Atim = fuse.NewTimespec(time.Now())
		stat.Nlink = 1
		return 0
	default:
		node, ok := fs.created[path]
		if ok {
			stat.Mode = fuse.S_IFREG | 0777
			stat.Mtim = node.attr.mtime
			stat.Ctim = node.attr.ctime
			stat.Atim = fuse.NewTimespec(time.Now())
			stat.Size = node.attr.len
			stat.Nlink = 1

		} else {
			log.Printf("object does not exist\n")
			return -fuse.ENOENT
		}
	}
	log.Printf("file ret 0")
	return 0
}

func (fs *RamFs) Read(path string, buff []byte, ofst int64, fh uint64) (n int) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	log.Printf("reading file\n")
	node, ok := fs.created[path]
	if !ok {
		return -fuse.ENOENT
	}
	endofst := ofst + int64(len(buff))
	node.attr.atime = fuse.NewTimespec(time.Now())
	if endofst > node.attr.len {
		endofst = node.attr.len
	}
	if ofst > node.attr.len {
		return -fuse.EFAULT
	}
	log.Printf("%s\n", node.data[ofst:endofst])
	n = copy(buff, node.data[ofst:endofst])
	return n
}

func (fs *RamFs) Write(path string, buff []byte, ofst int64, fh uint64) (n int) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	log.Printf("writing into file\n")
	node, ok := fs.created[path]
	if !ok {
		return -fuse.ENOENT
	}
	node.attr.atime = fuse.NewTimespec(time.Now())
	if ofst > node.attr.len {
		return -fuse.EFAULT
	}
	if node.attr.len > 0 {
		node.data = node.data[:ofst]
	}
	if node.data == "" {
		node.data = string(buff)
	} else {
		node.data = node.data + string(buff)
	}
	fmt.Printf("buff %s data %s, len %d\n", string(buff), node.data, int64(len(node.data)))
	node.attr.len = int64(len(node.data))
	return len(buff)
}

func (fs *RamFs) Mknod(path string, mode uint32, dev uint64) (errc int) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	log.Printf("creating a file\n")
	n := fs.created[path]
	if n != nil {
		log.Printf("file exists %s\n", path)
		return -fuse.EEXIST
	}
	attr := Attrs{
		len:   0,
		mtime: fuse.NewTimespec(time.Now()),
		ctime: fuse.NewTimespec(time.Now()),
		atime: fuse.NewTimespec(time.Now()),
		cnt:   1,
	}
	node := &Node{
		attr: attr,
		ino:  fs.ino,
		data: "",
	}
	fs.ino++
	fs.created[path] = node
	return 0
}

func (fs *RamFs) Utimens(path string, tmsp []fuse.Timespec) (errc int) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	node, ok := fs.created[path]
	if !ok {
		return -fuse.ENOENT
	}
	node.attr.ctime = fuse.Now()
	if tmsp == nil {
		tmsp0 := node.attr.ctime
		tmsa := [2]fuse.Timespec{tmsp0, tmsp0}
		tmsp = tmsa[:]
	}
	node.attr.atime = tmsp[0]
	node.attr.mtime = tmsp[1]
	return 0
}
