package main

import (
	"flag"
	"fmt"
	"os"
	"ss3fs/ss3fs"

	"github.com/winfsp/cgofuse/fuse"
)

type Params struct {
	Access     *string
	Secret     *string
	Bucket     *string
	EndPoint   *string
	MountPoint *string
	Region     *string
}

var Flags *flag.FlagSet = flag.NewFlagSet("", flag.ExitOnError)

func parseParams() *Params {
	params := Params{
		Access:     Flags.String("k", "", "Access key"),
		Secret:     Flags.String("s", "", "Secret key"),
		Bucket:     Flags.String("b", "", "Bucket"),
		EndPoint:   Flags.String("e", "", "Endpoint to the object storage, with http/https and port"),
		MountPoint: Flags.String("m", "", "Mount point"),
		Region:     Flags.String("r", "us-west-2", "AWS region"),
	}
	Flags.Parse(os.Args[1:])
	return &params
}

/* simple implementation for s3fs */
/* */
func main() {
	param := parseParams()
	fs, err := ss3fs.NewSs3fs(param.Access, param.Secret, param.Region, param.Bucket, param.EndPoint)
	if err != nil {
		fmt.Printf("Can't initialize ss3fs, error %v\n", err)
		return
	}

	host := fuse.NewFileSystemHost(fs)
	host.Mount(*param.MountPoint, nil)
}
