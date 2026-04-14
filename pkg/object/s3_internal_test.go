package object

import (
	"context"
	"strings"
	"testing"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

func TestS3ConfigDisablesSharedFilesWithoutExplicitProfile(t *testing.T) {
	_, err := NewS3StoreFromConfig(context.Background(), S3Config{Bucket: "bucket"})
	if err == nil {
		t.Fatalf("expected missing credentials error")
	}
	const want = "credentials not configured"
	if got := err.Error(); !strings.Contains(got, want) {
		t.Fatalf("expected %q in error, got %q", want, got)
	}
}

func TestS3ConfigKeepsSharedFilesWhenProfileExplicit(t *testing.T) {
	var opts awsconfig.LoadOptions
	for _, fn := range s3LoadOptions(S3Config{Profile: "prod"}) {
		if err := fn(&opts); err != nil {
			t.Fatalf("apply load option: %v", err)
		}
	}

	if opts.SharedConfigProfile != "prod" {
		t.Fatalf("expected explicit shared config profile, got %q", opts.SharedConfigProfile)
	}
}
