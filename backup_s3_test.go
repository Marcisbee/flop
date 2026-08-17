package flop

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3transfer "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type multipartUploadRecorder struct {
	mu                     sync.Mutex
	putObjectCalls         int
	createMultipartCalls   int
	uploadPartCalls        int
	completeMultipartCalls int
	abortMultipartCalls    int
	uploadedBytes          int64
}

func (r *multipartUploadRecorder) PutObject(_ context.Context, input *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	body, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.putObjectCalls++
	r.uploadedBytes += int64(len(body))
	r.mu.Unlock()
	return &awss3.PutObjectOutput{}, nil
}

func (r *multipartUploadRecorder) CreateMultipartUpload(_ context.Context, _ *awss3.CreateMultipartUploadInput, _ ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
	r.mu.Lock()
	r.createMultipartCalls++
	r.mu.Unlock()
	return &awss3.CreateMultipartUploadOutput{UploadId: aws.String("upload-id")}, nil
}

func (r *multipartUploadRecorder) UploadPart(_ context.Context, input *awss3.UploadPartInput, _ ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
	body, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.uploadPartCalls++
	r.uploadedBytes += int64(len(body))
	part := r.uploadPartCalls
	r.mu.Unlock()
	return &awss3.UploadPartOutput{ETag: aws.String(fmt.Sprintf("part-%d", part))}, nil
}

func (r *multipartUploadRecorder) CompleteMultipartUpload(_ context.Context, _ *awss3.CompleteMultipartUploadInput, _ ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error) {
	r.mu.Lock()
	r.completeMultipartCalls++
	r.mu.Unlock()
	return &awss3.CompleteMultipartUploadOutput{}, nil
}

func (r *multipartUploadRecorder) AbortMultipartUpload(_ context.Context, _ *awss3.AbortMultipartUploadInput, _ ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
	r.mu.Lock()
	r.abortMultipartCalls++
	r.mu.Unlock()
	return &awss3.AbortMultipartUploadOutput{}, nil
}

func (r *multipartUploadRecorder) GetObject(_ context.Context, _ *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	return &awss3.GetObjectOutput{}, nil
}

func (r *multipartUploadRecorder) HeadObject(_ context.Context, _ *awss3.HeadObjectInput, _ ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	return &awss3.HeadObjectOutput{}, nil
}

func (r *multipartUploadRecorder) ListObjectsV2(_ context.Context, _ *awss3.ListObjectsV2Input, _ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	return &awss3.ListObjectsV2Output{}, nil
}

func TestUploadS3BackupUsesMultipartForLargeFiles(t *testing.T) {
	recorder := &multipartUploadRecorder{}
	bodySize := backupS3UploadPartSize + 1
	body := bytes.NewReader(make([]byte, bodySize))

	err := uploadS3Backup(context.Background(), recorder, "backups", "large.zip", body)
	if err != nil {
		t.Fatalf("upload large backup: %v", err)
	}

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.putObjectCalls != 0 {
		t.Fatalf("expected multipart upload, got %d PutObject calls", recorder.putObjectCalls)
	}
	if recorder.createMultipartCalls != 1 {
		t.Fatalf("expected one multipart initialization, got %d", recorder.createMultipartCalls)
	}
	if recorder.uploadPartCalls != 2 {
		t.Fatalf("expected two uploaded parts, got %d", recorder.uploadPartCalls)
	}
	if recorder.completeMultipartCalls != 1 {
		t.Fatalf("expected one multipart completion, got %d", recorder.completeMultipartCalls)
	}
	if recorder.abortMultipartCalls != 0 {
		t.Fatalf("expected no multipart abort, got %d", recorder.abortMultipartCalls)
	}
	if recorder.uploadedBytes != int64(bodySize) {
		t.Fatalf("expected %d uploaded bytes, got %d", bodySize, recorder.uploadedBytes)
	}
}

var _ s3transfer.S3APIClient = (*multipartUploadRecorder)(nil)

func TestS3HTTPClientHasNoWholeRequestTimeout(t *testing.T) {
	client := newS3HTTPClient()
	if client.Timeout != 0 {
		t.Fatalf("expected no whole-request timeout, got %s", client.Timeout)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", client.Transport)
	}
	if transport.ResponseHeaderTimeout != backupS3HeaderTimeout {
		t.Fatalf("expected response header timeout %s, got %s", backupS3HeaderTimeout, transport.ResponseHeaderTimeout)
	}
}
