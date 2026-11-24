package dynamodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// Use a range where a JSON float64 checkpoint rounds to a different integer.
const cursorBase = int64(1 << 53)

func ExecuteQuery(ctx context.Context, t *testing.T, conf *testutils.TestConfig, operation string) {
	t.Helper()

	config := conf.SourceBaseConfig
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(config.String("region")),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")))
	require.NoError(t, err, "failed to load AWS config")
	client := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(config.Host("endpoint"))
	})

	table := conf.GetTableName()

	switch operation {
	case "create":
		createTable(ctx, t, client, table)
	case "drop":
		deleteTable(ctx, t, client, table)
	case "drop-all":
		out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
		require.NoError(t, err, "failed to list tables")
		for _, name := range out.TableNames {
			deleteTable(ctx, t, client, name)
		}
	case "clean":
		scanAndDeleteAll(ctx, t, client, table)
	case "add":
		for i := 1; i <= 5; i++ {
			putItem(ctx, t, client, table, seedItem(int64(i), cursorBase+int64(i)))
		}
		// filtered row: id_int fails the suite filter, so it never reaches the destination
		filtered := seedItem(999, -1)
		filtered["id_int"] = &ddbtypes.AttributeValueMemberN{Value: "5000"}
		putItem(ctx, t, client, table, filtered)
	case "insert":
		putItem(ctx, t, client, table, seedItem(6, cursorBase+6))
		filtered := seedItem(999, cursorBase+6)
		filtered["id_int"] = &ddbtypes.AttributeValueMemberN{Value: "5000"}
		putItem(ctx, t, client, table, filtered)
	case "update":
		item := seedItem(1, cursorBase+7)
		item["id_bigint"] = &ddbtypes.AttributeValueMemberN{Value: "987654321098765"}
		item["id_int"] = &ddbtypes.AttributeValueMemberN{Value: "200"}
		item["id_double"] = &ddbtypes.AttributeValueMemberN{Value: "202.456"}
		item["id_bool"] = &ddbtypes.AttributeValueMemberBOOL{Value: false}
		item["id_timestamp"] = &ddbtypes.AttributeValueMemberS{Value: "2024-07-01T15:30:00Z"}
		item["name_varchar"] = &ddbtypes.AttributeValueMemberS{Value: "updated varchar"}
		item["excludedColumn"] = &ddbtypes.AttributeValueMemberN{Value: "102"}
		// a column discover has not seen: the update sync must carry it through
		item["includedColumn"] = &ddbtypes.AttributeValueMemberN{Value: "202"}
		putItem(ctx, t, client, table, item)
	case "delete":
		_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(table),
			Key:       map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberN{Value: "1"}},
		})
		require.NoError(t, err, "failed to delete item")
	default:
		t.Fatalf("unsupported operation %q", operation)
	}
}

// seedItem is the item every seeded row starts from; callers set id and the cursor.
func seedItem(id, cursor int64) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"id":             &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", id)},
		"id_cursor":      &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", cursor)},
		"id_bigint":      &ddbtypes.AttributeValueMemberN{Value: "123456789012345"},
		"id_int":         &ddbtypes.AttributeValueMemberN{Value: "100"},
		"id_double":      &ddbtypes.AttributeValueMemberN{Value: "123.456"},
		"id_bool":        &ddbtypes.AttributeValueMemberBOOL{Value: true},
		"id_timestamp":   &ddbtypes.AttributeValueMemberS{Value: "2023-01-01T12:00:00Z"},
		"id_nil":         &ddbtypes.AttributeValueMemberNULL{Value: true},
		"id_nested":      &ddbtypes.AttributeValueMemberM{Value: nestedAttribute()},
		"id_list":        &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{&ddbtypes.AttributeValueMemberS{Value: "a"}, &ddbtypes.AttributeValueMemberN{Value: "1"}}},
		"id_ss":          &ddbtypes.AttributeValueMemberSS{Value: []string{"x", "y"}},
		"name_varchar":   &ddbtypes.AttributeValueMemberS{Value: "varchar_val"},
		"excludedColumn": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}
}

func nestedAttribute() map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"nested_string":    &ddbtypes.AttributeValueMemberS{Value: "nested_value"},
		"nested_int":       &ddbtypes.AttributeValueMemberN{Value: "42"},
		"nested_timestamp": &ddbtypes.AttributeValueMemberS{Value: "2023-01-01T12:00:00Z"},
	}
}

func createTable(ctx context.Context, t *testing.T, client *dynamodb.Client, table string) {
	t.Helper()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(table),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeN},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	if err != nil {
		require.Contains(t, err.Error(), "ResourceInUseException", "failed to create table %s", table)
	}
}

func deleteTable(ctx context.Context, t *testing.T, client *dynamodb.Client, table string) {
	t.Helper()
	_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(table)})
	if err != nil {
		require.Contains(t, err.Error(), "ResourceNotFoundException", "failed to delete table %s", table)
	}
}

func scanAndDeleteAll(ctx context.Context, t *testing.T, client *dynamodb.Client, table string) {
	t.Helper()
	paginator := dynamodb.NewScanPaginator(client, &dynamodb.ScanInput{
		TableName:            aws.String(table),
		ProjectionExpression: aws.String("id"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		require.NoError(t, err, "failed to scan table %s for clean", table)
		for _, item := range page.Items {
			_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(table),
				Key:       map[string]ddbtypes.AttributeValue{"id": item["id"]},
			})
			require.NoError(t, err, "failed to delete item during clean")
		}
	}
}

func putItem(ctx context.Context, t *testing.T, client *dynamodb.Client, table string, item map[string]ddbtypes.AttributeValue) {
	t.Helper()
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(table), Item: item})
	require.NoError(t, err, "failed to put item into %s", table)
}

var ExpectedDynamoData = map[string]interface{}{
	"id_bigint":    int64(123456789012345),
	"id_int":       int64(100),
	"id_double":    testutils.Decimal("123.456"),
	"id_bool":      true,
	"id_timestamp": arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"id_nested":    `{"nested_int":42,"nested_string":"nested_value","nested_timestamp":"2023-01-01T12:00:00Z"}`,
	"id_list":      `["a",1]`,
	"id_ss":        `["x","y"]`,
	"name_varchar": "varchar_val",
}

var ExpectedUpdatedData = map[string]interface{}{
	"id":             int64(1),
	"id_cursor":      cursorBase + 7,
	"id_bigint":      int64(987654321098765),
	"id_int":         int64(200),
	"id_double":      testutils.Decimal("202.456"),
	"id_bool":        false,
	"id_timestamp":   arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"id_nested":      `{"nested_int":42,"nested_string":"nested_value","nested_timestamp":"2023-01-01T12:00:00Z"}`,
	"id_list":        `["a",1]`,
	"id_ss":          `["x","y"]`,
	"name_varchar":   "updated varchar",
	"includedcolumn": int64(202),
}

var DynamoToDestinationSchema = map[string]string{
	"id":           "bigint",
	"id_cursor":    "bigint",
	"id_bigint":    "bigint",
	"id_int":       "bigint",
	"id_double":    "decimal(6,3)",
	"id_bool":      "boolean",
	"id_timestamp": "timestamp",
	"id_nil":       "null",
	"id_nested":    "string",
	"id_list":      "string",
	"id_ss":        "string",
	"name_varchar": "string",
}

var UpdatedDynamoToDestinationSchema = func() map[string]string {
	schema := make(map[string]string, len(DynamoToDestinationSchema)+1)
	for k, v := range DynamoToDestinationSchema {
		schema[k] = v
	}
	schema["includedcolumn"] = "bigint"
	return schema
}()

// DynamoTypeMapping maps every type DynamoToDestinationSchema declares to the type it lands as.
func DynamoTypeMapping() map[string]string {
	return map[string]string{
		"bigint":       "bigint",
		"boolean":      "boolean",
		"decimal(6,3)": "decimal(6,3)",
		"null":         "string",
		"string":       "string",
		"timestamp":    "timestamp",
	}
}

// DynamoParquetTypeMapping is DynamoTypeMapping for Parquet, which writes an always-null field as
// untyped bytes that Spark reads as binary.
func DynamoParquetTypeMapping() map[string]string {
	mapping := DynamoTypeMapping()
	mapping["null"] = "binary"
	return mapping
}
