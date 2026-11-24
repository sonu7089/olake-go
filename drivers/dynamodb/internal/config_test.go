package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config with credentials",
			config: Config{
				Region:          "us-east-1",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
			},
			wantErr: false,
		},
		{
			name:    "valid config with default credential chain",
			config:  Config{Region: "us-east-1"},
			wantErr: false,
		},
		{
			name:    "custom endpoint without region",
			config:  Config{Endpoint: "http://localhost:8000"},
			wantErr: false,
		},
		{
			name:    "missing region without custom endpoint",
			config:  Config{},
			wantErr: true,
			errMsg:  "region is required when not using custom endpoint",
		},
		{
			name: "partial credentials - only access key",
			config: Config{
				Region:      "us-east-1",
				AccessKeyID: "test-access-key",
			},
			wantErr: true,
			errMsg:  "access_key_id and secret_access_key must be provided together",
		},
		{
			name: "partial credentials - only secret key",
			config: Config{
				Region:          "us-east-1",
				SecretAccessKey: "test-secret-key",
			},
			wantErr: true,
			errMsg:  "access_key_id and secret_access_key must be provided together",
		},
		{
			name: "session token without keys",
			config: Config{
				Region:       "us-east-1",
				SessionToken: "test-token",
			},
			wantErr: true,
			errMsg:  "session_token requires access_key_id and secret_access_key",
		},
		{
			name: "session token with keys",
			config: Config{
				Region:          "us-east-1",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				SessionToken:    "test-token",
			},
			wantErr: false,
		},
		{
			name: "negative max threads",
			config: Config{
				Region:     "us-east-1",
				MaxThreads: -1,
			},
			wantErr: true,
		},
		{
			name: "negative retry count",
			config: Config{
				Region:     "us-east-1",
				RetryCount: -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfigValidateDefaults(t *testing.T) {
	config := Config{Region: "us-east-1"}
	require.NoError(t, config.Validate())
	assert.Equal(t, constants.DefaultThreadCount, config.MaxThreads)
	assert.Equal(t, constants.DefaultRetryCount, config.RetryCount)
}
