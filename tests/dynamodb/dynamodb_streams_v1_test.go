package dynamodb

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// Streams v1 (--catalog / streams.json) coverage, run alongside the streams v2 suites.
// Delete this file when streams v1 support is removed.

func TestDynamodbStreamsV1Discover(t *testing.T) {
	dynamodbBaseConfig(t, testutils.WithStreamsV1()).TestDiscover(t)
}

func TestDynamodbStreamsV1Sync(t *testing.T) {
	t.Parallel()
	dynamodbSyncConfig(t, testutils.WithStreamsV1()).TestSync(t)
}
