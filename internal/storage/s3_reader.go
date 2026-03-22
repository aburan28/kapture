package storage

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3GetObjectAPI interface {
	ListObjectsV2(ctx context.Context, params *awss3.ListObjectsV2Input, optFns ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *awss3.GetObjectInput, optFns ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
}

// S3ObjectStore implements ObjectStore for Amazon S3.
type S3ObjectStore struct {
	client s3GetObjectAPI
	bucket string
}

// S3ReaderConfig configures an S3-backed ReaderFactory.
type S3ReaderConfig struct {
	Bucket string
	Region string
	Prefix string
	Client s3GetObjectAPI
}

// NewS3ReaderFactory creates a ReaderFactory that reads captured data from S3.
func NewS3ReaderFactory(ctx context.Context, cfg S3ReaderConfig) (ReaderFactory, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 bucket is required")
	}
	if cfg.Client == nil {
		loadOptions := make([]func(*awscfg.LoadOptions) error, 0, 1)
		if cfg.Region != "" {
			loadOptions = append(loadOptions, awscfg.WithRegion(cfg.Region))
		}
		awsCfg, err := awscfg.LoadDefaultConfig(ctx, loadOptions...)
		if err != nil {
			return nil, fmt.Errorf("load aws config: %w", err)
		}
		cfg.Client = awss3.NewFromConfig(awsCfg)
	}

	store := &S3ObjectStore{client: cfg.Client, bucket: cfg.Bucket}
	return NewObjectStoreReaderFactory(store, strings.Trim(cfg.Prefix, "/")), nil
}

func (s *S3ObjectStore) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	var continuationToken *string

	for {
		output, err := s.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list s3 objects with prefix %q: %w", prefix, err)
		}

		for _, obj := range output.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	return keys, nil
}

func (s *S3ObjectStore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get s3 object %q: %w", key, err)
	}
	return output.Body, nil
}
