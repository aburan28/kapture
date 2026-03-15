package storage

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3PutObjectAPI interface {
	PutObject(ctx context.Context, params *awss3.PutObjectInput, optFns ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
}

type S3Config struct {
	Bucket         string
	Region         string
	Prefix         string
	AgentPod       string
	FlushInterval  time.Duration
	MaxBufferBytes int
	Client         s3PutObjectAPI
}

func (S3Config) isStorageConfig() {}

type S3WriterFactory struct{ config S3Config }

type S3Writer struct{ *bufferedObjectWriter }

func NewS3WriterFactory(ctx context.Context, cfg S3Config) (*S3WriterFactory, error) {
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
	return &S3WriterFactory{config: cfg}, nil
}

func (f *S3WriterFactory) NewWriter(_ context.Context, captureID string) (Writer, error) {
	if captureID == "" {
		return nil, fmt.Errorf("capture ID is required")
	}

	upload := func(ctx context.Context, objectName string, payload []byte) error {
		_, err := f.config.Client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket:          aws.String(f.config.Bucket),
			Key:             aws.String(objectName),
			Body:            bytes.NewReader(payload),
			ContentType:     aws.String(contentTypeJSONL),
			ContentEncoding: aws.String(contentEncodingGZIP),
		})
		if err != nil {
			return fmt.Errorf("put s3 object %q: %w", objectName, err)
		}
		return nil
	}

	return &S3Writer{bufferedObjectWriter: newBufferedObjectWriter(
		captureID,
		f.config.FlushInterval,
		f.config.MaxBufferBytes,
		newCloudObjectName(f.config.Prefix, captureID, f.config.AgentPod),
		upload,
	)}, nil
}
