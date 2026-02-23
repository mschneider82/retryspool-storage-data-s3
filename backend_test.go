package s3

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestS3DataBackend(t *testing.T) {
	// Check if local S3 (MinIO) is available
	endpoint := "localhost:9000"
	conn, err := net.DialTimeout("tcp", endpoint, 100*time.Millisecond)
	if err != nil {
		t.Skip("Local S3 (MinIO) not available at localhost:9000")
	}
	conn.Close()

	ctx := context.Background()
	bucket := "test-retryspool-data"

	// Configure S3 client for MinIO
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	if err != nil {
		t.Fatalf("Failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + endpoint)
		o.UsePathStyle = true
	})

	// Ensure bucket exists
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") && !strings.Contains(err.Error(), "BucketAlreadyExists") {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	backend := NewBackend(client, bucket, WithKeyPrefix("test-prefix"))
	defer backend.Close()

	// Test data
	messageID := "test-message-s3-123"
	testData := "This is test message data that will be stored in S3."

	// Test StoreData
	size, err := backend.StoreData(ctx, messageID, strings.NewReader(testData))
	if err != nil {
		t.Fatalf("Failed to store data: %v", err)
	}

	expectedSize := int64(len(testData))
	if size != expectedSize {
		t.Errorf("Size mismatch: expected %d, got %d", expectedSize, size)
	}

	// Test GetDataReader
	reader, err := backend.GetDataReader(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get data reader: %v", err)
	}
	defer reader.Close()

	retrievedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if string(retrievedData) != testData {
		t.Errorf("Data mismatch: expected %s, got %s", testData, string(retrievedData))
	}

	// Test GetDataWriter
	messageID2 := "test-message-s3-456"
	testData2 := "This is another test message using the writer interface for S3."

	writer, err := backend.GetDataWriter(ctx, messageID2)
	if err != nil {
		t.Fatalf("Failed to get data writer: %v", err)
	}

	_, err = writer.Write([]byte(testData2))
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify written data
	reader2, err := backend.GetDataReader(ctx, messageID2)
	if err != nil {
		t.Fatalf("Failed to get data reader for written data: %v", err)
	}
	defer reader2.Close()

	retrievedData2, err := io.ReadAll(reader2)
	if err != nil {
		t.Fatalf("Failed to read written data: %v", err)
	}

	if string(retrievedData2) != testData2 {
		t.Errorf("Written data mismatch: expected %s, got %s", testData2, string(retrievedData2))
	}

	// Test DeleteData
	err = backend.DeleteData(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to delete data: %v", err)
	}

	// Clean up second message
	_ = backend.DeleteData(ctx, messageID2)

	// Verify deletion
	_, err = backend.GetDataReader(ctx, messageID)
	if err == nil {
		t.Errorf("Expected error getting deleted data, but got nil")
	} else if !strings.Contains(err.Error(), "not found") {
		t.Errorf("Expected 'not found' error, got: %v", err)
	}

	// Optional: Delete bucket at the end if you want a full cleanup
	// _, _ = client.DeleteBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
}

func TestS3DataBackend_LargeData(t *testing.T) {
	// Check if local S3 (MinIO) is available
	endpoint := "localhost:9000"
	conn, err := net.DialTimeout("tcp", endpoint, 100*time.Millisecond)
	if err != nil {
		t.Skip("Local S3 (MinIO) not available at localhost:9000")
	}
	conn.Close()

	ctx := context.Background()
	bucket := "test-retryspool-large"

	cfg, _ := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + endpoint)
		o.UsePathStyle = true
	})

	_, _ = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})

	backend := NewBackend(client, bucket)
	defer backend.Close()

	// 6MB of data (larger than default S3 multi-part chunk size of 5MB)
	largeData := bytes.Repeat([]byte("ABCDEFGH"), 1024*1024*6/8)
	messageID := "large-message-s3"

	size, err := backend.StoreData(ctx, messageID, bytes.NewReader(largeData))
	if err != nil {
		t.Fatalf("Failed to store large data: %v", err)
	}

	if size != int64(len(largeData)) {
		t.Errorf("Size mismatch: expected %d, got %d", len(largeData), size)
	}

	reader, err := backend.GetDataReader(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get large data reader: %v", err)
	}
	defer reader.Close()

	// Verify first few bytes to avoid loading everything if it fails
	buf := make([]byte, 8)
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		t.Fatalf("Failed to read start of large data: %v", err)
	}
	if string(buf) != "ABCDEFGH" {
		t.Errorf("Data content mismatch at start")
	}

	// Clean up
	_ = backend.DeleteData(ctx, messageID)
}
