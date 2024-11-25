package ss3fs

import (
	"context"
	"errors"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/winfsp/cgofuse/fuse"
)

func BucketExists(bucket string, clnt *s3.Client, ctx *context.Context) (bool, error) {
	exists := true
	queryBucket := &s3.HeadBucketInput{Bucket: aws.String(bucket)}
	_, err := clnt.HeadBucket(*ctx, queryBucket)
	if err != nil {
		exists = false
		var apiErr smithy.APIError
		/* check if request failed */
		if errors.As(err, &apiErr) {
			switch apiErr.(type) {
			case *types.NotFound:
				err = nil
			default:
				log.Printf("Request failed with error %v\n", err)
			}
		}
	}
	return exists, err
}

func ObjectExist(bucket string, object string, clnt *s3.Client, ctx *context.Context, attr *Attrs) (bool, error) {
	exists := true
	queryObject := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	res, err := clnt.HeadObject(*ctx, queryObject)
	if err != nil {
		exists = false
		var apiErr smithy.APIError
		/* check if request failed */
		if errors.As(err, &apiErr) {
			switch apiErr.(type) {
			case *types.NotFound:
				err = nil
			default:
				log.Printf("Request failed with error %v\n", err)
			}
		}
	}
	/* fill attrs if needed */
	if exists && attr != nil {
		attr.stat.Size = *res.ContentLength
		attr.stat.Mtim = fuse.NewTimespec(*res.LastModified)
		attr.stat.Ctim = fuse.NewTimespec(*res.LastModified)
	}
	return exists, err
}
