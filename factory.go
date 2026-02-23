package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	datastorage "schneider.vip/retryspool/storage/data"
)

// Factory creates S3 data storage backends
type Factory struct {
	clientOptions  []func(*s3.Options)
	backendOptions []func(*Backend)
	bucket         string
	client         *s3.Client
}

// NewFactory creates a new S3 data storage factory
func NewFactory(bucket string) *Factory {
	return &Factory{
		bucket: bucket,
	}
}

// WithClient sets a pre-configured S3 client
func (f *Factory) WithClient(client *s3.Client) *Factory {
	f.client = client
	return f
}

// WithClientOptions adds options for the S3 client (only used if client is not provided)
func (f *Factory) WithClientOptions(opts ...func(*s3.Options)) *Factory {
	f.clientOptions = append(f.clientOptions, opts...)
	return f
}

// WithBackendOptions adds options for the S3 backend
func (f *Factory) WithBackendOptions(opts ...func(*Backend)) *Factory {
	f.backendOptions = append(f.backendOptions, opts...)
	return f
}

// Create creates a new S3 data storage backend
func (f *Factory) Create() (datastorage.Backend, error) {
	client := f.client
	if client == nil {
		// Create a default client
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to load aws config: %w", err)
		}
		client = s3.NewFromConfig(cfg, f.clientOptions...)
	}

	return NewBackend(client, f.bucket, f.backendOptions...), nil
}

// Name returns the factory name
func (f *Factory) Name() string {
	return "s3-data"
}
