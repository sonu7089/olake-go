package dynamodb

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// dynamodbTestConfig builds the config every dynamodb suite shares: the source, the namespace
// and the stream settings. DynamoDB has no CDC (Streams' 24h retention cannot guarantee gap-free
// capture), so the suites run full load + incremental only.
func dynamodbTestConfig(t *testing.T, opts ...testutils.TestConfigOption) *testutils.TestConfig {
	cfg, err := testutils.NewTestConfig(t, constants.DynamoDB, "dynamodb", ExecuteQuery, opts...)
	require.NoError(t, err, "failed to build the test config")
	cfg.CursorField = "id_cursor"
	cfg.PartitionRegex = "/{id,identity}"
	cfg.ColumnToExclude = "excludedColumn"
	cfg.SkipSchemaEvolution = true
	// All the seeded rows except id=999 satisfy this filter.
	cfg.FilterConfig = `{
		"logical_operator": "And",
		"conditions": [
			{
				"column": "id_int",
				"operator": "<",
				"value": 1000
			},
			{
				"column": "name_varchar",
				"operator": "!=",
				"value": ""
			}
		]
	}`
	return cfg
}

// dynamodbBaseConfig is dynamodbTestConfig with the integration suites' expected data.
func dynamodbBaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.TestHandler {
	return &integration.TestHandler{
		TestConfig:                dynamodbTestConfig(t, opts...),
		ExpectedData:              ExpectedDynamoData,
		DestinationDataTypeSchema: DynamoToDestinationSchema,
		TypeMapping:               DynamoTypeMapping(),
		ParquetTypeMapping:        DynamoParquetTypeMapping(),
	}
}

func TestDynamodbDiscover(t *testing.T) {
	dynamodbBaseConfig(t).TestDiscover(t)
}

// dynamodbSyncConfig adds to dynamodbBaseConfig what the sync suites verify after the update sync.
func dynamodbSyncConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.TestHandler {
	cfg := dynamodbBaseConfig(t, opts...)
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedDynamoToDestinationSchema
	return cfg
}

func TestDynamodbSync(t *testing.T) {
	t.Parallel()
	dynamodbSyncConfig(t).TestSync(t)
}
