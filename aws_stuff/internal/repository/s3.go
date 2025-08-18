package repository

import (
	"context"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Repo struct {
	client *s3.S3
}

func NewS3Repo(client *s3.S3) *S3Repo {
	return &S3Repo{client: client}
}

func (r *S3Repo) GetObjectString(ctx context.Context, bucket string, key string) (string, error) {
	out, err := r.client.GetObjectWithContext(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return "", err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
