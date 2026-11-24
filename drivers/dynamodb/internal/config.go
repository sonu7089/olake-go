package driver

import (
	"fmt"
	"net/url"
	"regexp"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
)

const maxScanSegments = 1_000_000

// Config represents the configuration for the DynamoDB source connector
type Config struct {
	// ===== AWS Connection Configuration =====
	Region          string `json:"region"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token"`
	Endpoint        string `json:"endpoint"` // Optional: for DynamoDB Local or other compatible endpoints

	// ===== Table Filtering =====
	TablePattern string `json:"table_pattern"` // Regex pattern to filter tables (optional)

	// ===== Read Configuration =====
	ConsistentRead bool `json:"consistent_read"`

	// ===== Performance Configuration =====
	MaxThreads int `json:"max_threads"` // Parallel scan segments and discover concurrency
	RetryCount int `json:"retry_count"` // Number of retries for failed operations
}

// Validate validates the DynamoDB configuration
func (c *Config) Validate() error {
	// Region is required unless a custom endpoint is supplied (DynamoDB Local still
	// needs a region for request signing; Setup defaults it to us-east-1).
	if c.Endpoint == "" && c.Region == "" {
		return errs.Precondition(errs.ConfigInvalid, codeRegionMissing,
			fmt.Errorf("region is required when not using custom endpoint"))
	}
	if c.Endpoint != "" {
		endpoint, err := url.Parse(c.Endpoint)
		if err != nil || endpoint.Hostname() == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") ||
			endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
			return errs.Precondition(errs.ConfigInvalid, "dynamodb.endpoint_invalid",
				fmt.Errorf("endpoint must be an absolute HTTP or HTTPS URL without credentials, query parameters, or a fragment"))
		}
	}
	if _, err := regexp.Compile(c.TablePattern); err != nil {
		return errs.Precondition(errs.ConfigInvalid, codeTablePatternInvalid,
			fmt.Errorf("failed to compile table_pattern regex: %w", err))
	}

	// Credentials must be provided together or omitted together; when omitted the
	// driver falls back to the default credential chain (IAM roles, env vars, etc.)
	if (c.AccessKeyID != "") != (c.SecretAccessKey != "") {
		return errs.Precondition(errs.ConfigInvalid, codeIncompleteAuth,
			fmt.Errorf("access_key_id and secret_access_key must be provided together or both omitted (for IAM role authentication)"))
	}

	if c.SessionToken != "" && c.AccessKeyID == "" {
		return errs.Precondition(errs.ConfigInvalid, codeIncompleteAuth,
			fmt.Errorf("session_token requires access_key_id and secret_access_key"))
	}

	if err := utils.ApplyMaxThreadsDefault(&c.MaxThreads); err != nil {
		return err
	}
	if c.MaxThreads > maxScanSegments {
		return errs.Precondition(errs.ConfigInvalid, "dynamodb.max_threads_invalid",
			fmt.Errorf("max_threads must be at most %d", maxScanSegments))
	}

	if err := utils.ApplyRetryCountDefault(&c.RetryCount); err != nil {
		return err
	}

	return nil
}
