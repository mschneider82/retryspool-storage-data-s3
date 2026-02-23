package s3

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Backend implements datastorage.Backend for S3-compatible storage
type Backend struct {
	client       *s3.Client
	uploader     *manager.Uploader
	bucket       string
	keyPrefix    string
	storageClass types.StorageClass
}

// NewBackend creates a new S3 data storage backend
func NewBackend(client *s3.Client, bucket string, options ...func(*Backend)) *Backend {
	b := &Backend{
		client:   client,
		uploader: manager.NewUploader(client),
		bucket:   bucket,
	}

	for _, opt := range options {
		opt(b)
	}

	return b
}

// WithKeyPrefix sets a custom prefix for message keys
func WithKeyPrefix(prefix string) func(*Backend) {
	return func(b *Backend) {
		b.keyPrefix = prefix
	}
}

// WithStorageClass sets the S3 storage class
func WithStorageClass(storageClass types.StorageClass) func(*Backend) {
	return func(b *Backend) {
		b.storageClass = storageClass
	}
}

// StoreData stores message data in S3
func (b *Backend) StoreData(ctx context.Context, messageID string, data io.Reader) (int64, error) {
	key := b.objectKey(messageID)

	// Use the uploader for efficient multi-part upload if needed
	input := &s3.PutObjectInput{
		Bucket:       aws.String(b.bucket),
		Key:          aws.String(key),
		Body:         data,
		StorageClass: b.storageClass,
		Metadata: map[string]string{
			"message-id": messageID,
			"created":    time.Now().Format(time.RFC3339),
		},
	}

	// manager.Uploader handles multi-part upload automatically for large objects
	_, err := b.uploader.Upload(ctx, input)
	if err != nil {
		return 0, fmt.Errorf("failed to upload data for message %s to s3: %w", messageID, err)
	}

	// We might want to know the size, but S3 Upload doesn't return it directly if it was a stream.
	// We can get it via HeadObject if needed, but for StoreData return, it is often just copied size.
	// However, io.Copy is already used inside Upload for reading from 'data'.
	// If the user needs the exact size, they might have to wrap the reader.

	// Since datastorage.Backend interface returns (int64, error), and we don't have it easily here,
	// we would ideally need a counting reader if the input reader doesn't have length.

	// A more robust way to get the size for the return value:
	head, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// If head fails, we still uploaded, so maybe just return 0 or an error?
		// Let's assume the upload was successful and we want to be correct.
		return 0, nil
	}

	if head.ContentLength == nil {
		return 0, nil
	}

	return *head.ContentLength, nil
}

// GetDataReader returns a reader for message data from S3
func (b *Backend) GetDataReader(ctx context.Context, messageID string) (io.ReadCloser, error) {
	key := b.objectKey(messageID)

	output, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Detect if it's a "Not Found" error
		errorMessage := err.Error()
		if strings.Contains(errorMessage, "NoSuchKey") || strings.Contains(errorMessage, "NotFound") || strings.Contains(errorMessage, "404") {
			return nil, fmt.Errorf("data for message %s not found in s3", messageID)
		}
		return nil, fmt.Errorf("failed to get data for message %s from s3: %w", messageID, err)
	}

	return output.Body, nil
}

// GetDataWriter returns a writer for message data using multi-part upload
func (b *Backend) GetDataWriter(ctx context.Context, messageID string) (io.WriteCloser, error) {
	key := b.objectKey(messageID)

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)

	// Start asynchronous upload
	go func() {
		defer pr.Close()
		input := &s3.PutObjectInput{
			Bucket:       aws.String(b.bucket),
			Key:          aws.String(key),
			Body:         pr,
			StorageClass: b.storageClass,
			Metadata: map[string]string{
				"message-id": messageID,
				"created":    time.Now().Format(time.RFC3339),
			},
		}

		_, err := b.uploader.Upload(ctx, input)
		errCh <- err
	}()

	return &s3WriterCloser{
		pw:    pw,
		errCh: errCh,
	}, nil
}

// DeleteData removes message data from S3
func (b *Backend) DeleteData(ctx context.Context, messageID string) error {
	key := b.objectKey(messageID)

	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete data for message %s from s3: %w", messageID, err)
	}

	return nil
}

// Close closes the S3 client resources (nothing much to do for SDK v2 client)
func (b *Backend) Close() error {
	return nil
}

func (b *Backend) objectKey(messageID string) string {
	if b.keyPrefix != "" {
		return fmt.Sprintf("%s/%s.data", b.keyPrefix, messageID)
	}
	return fmt.Sprintf("%s.data", messageID)
}

// s3WriterCloser implements io.WriteCloser for S3 streaming upload
type s3WriterCloser struct {
	pw    *io.PipeWriter
	errCh chan error
}

func (w *s3WriterCloser) Write(p []byte) (n int, err error) {
	return w.pw.Write(p)
}

func (w *s3WriterCloser) Close() error {
	// Signal EOF to the pipe reader
	if err := w.pw.Close(); err != nil {
		return err
	}
	// Wait for upload result
	return <-w.errCh
}
