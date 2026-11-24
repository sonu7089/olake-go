package driver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

type testHTTPClient func(*http.Request) (*http.Response, error)

func (f testHTTPClient) Do(req *http.Request) (*http.Response, error) { return f(req) }

func testClient(t *testing.T, respond func(string, map[string]any) string) *dynamodb.Client {
	t.Helper()
	return dynamodb.New(dynamodb.Options{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
		HTTPClient: testHTTPClient(func(req *http.Request) (*http.Response, error) {
			defer func() { require.NoError(t, req.Body.Close()) }()
			var input map[string]any
			require.NoError(t, json.NewDecoder(req.Body).Decode(&input))
			body := respond(req.Header.Get("X-Amz-Target"), input)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"application/x-amz-json-1.0"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		}),
	})
}

func TestDiscoverySamplesAcrossPages(t *testing.T) {
	scans := 0
	d := DynamoDB{config: &Config{ConsistentRead: true}}
	d.client = testClient(t, func(operation string, input map[string]any) string {
		switch operation {
		case "DynamoDB_20120810.DescribeTable":
			return `{"Table":{"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}`
		case "DynamoDB_20120810.Scan":
			require.Equal(t, true, input["ConsistentRead"])
			scans++
			if scans == 1 {
				require.Equal(t, float64(2), input["Limit"])
				return `{"Items":[{"pk":{"S":"a"},"sk":{"S":"1"},"mixed":{"S":"one"},"cursor":{"NULL":true}}],"LastEvaluatedKey":{"pk":{"S":"a"},"sk":{"S":"1"}}}`
			}
			require.Equal(t, 2, scans)
			require.Equal(t, float64(1), input["Limit"])
			require.Contains(t, input, "ExclusiveStartKey")
			return `{"Items":[{"pk":{"S":"a"},"sk":{"S":"2"},"mixed":{"N":"2"},"cursor":{"N":"10"},"late":{"BOOL":true}}],"LastEvaluatedKey":{"pk":{"S":"a"},"sk":{"S":"2"}}}`
		default:
			t.Fatalf("unexpected operation %s", operation)
			return ""
		}
	})
	stream, err := d.ProduceSampledSchema(t.Context(), types.StreamID{Namespace: namespace, Name: "table"}, 2)
	require.NoError(t, err)
	require.Equal(t, 2, scans)
	require.ElementsMatch(t, []string{"pk", "sk"}, stream.SourceDefinedPrimaryKey.Array())
	require.ElementsMatch(t, []string{"pk", "sk", "cursor"}, stream.AvailableCursorFields.Array())
	property, ok := stream.Schema.Properties.Load("late")
	require.True(t, ok)
	require.True(t, property.(*types.Property).Type.Exists(types.Null))
	require.Equal(t, types.FULLREFRESH, stream.SyncMode)
}

func TestIncrementalKeepsExactSourceCursors(t *testing.T) {
	const oldNumber = "9007199254740993"
	const newNumber = "9007199254740993.1"
	const laterNumber = "9007199254740993.2"
	const stamp = "2026-11-22T01:00:00Z"

	source := types.NewStream("table", namespace, nil).WithCursorField("sequence", "updated")
	source.UpsertField("sequence", types.Int64, false, false)
	source.UpsertField("updated", types.Timestamp, true, false)
	source.SyncMode = types.INCREMENTAL
	source.CursorField = "sequence:updated"
	stream := &types.ConfiguredStream{Stream: source}

	requests := 0
	d := DynamoDB{config: &Config{}}
	d.client = testClient(t, func(operation string, input map[string]any) string {
		require.Equal(t, "DynamoDB_20120810.Scan", operation)
		require.Equal(t, true, input["ConsistentRead"])
		requests++
		switch requests {
		case 1:
			require.Equal(t, "#cursor0, #cursor1", input["ProjectionExpression"])
			return `{"Items":[{"sequence":{"N":"9007199254740992.9"}},{"sequence":{"N":"` + oldNumber + `"}},{"sequence":{"N":"9e-130"}}]}`
		case 2:
			require.Equal(t, "#cursor0, #cursor1", input["ProjectionExpression"])
			return `{"Items":[{"sequence":{"N":"` + newNumber + `"},"updated":{"S":"` + stamp + `"}}]}`
		case 3:
			require.Equal(t, "(#cursor0 > :low0 AND #cursor0 <= :high0) OR #cursor1 <= :high1", input["FilterExpression"])
			require.Equal(t, map[string]any{
				":low0":  map[string]any{"N": oldNumber},
				":high0": map[string]any{"N": newNumber},
				":high1": map[string]any{"S": stamp},
			}, input["ExpressionAttributeValues"])
			return `{"Items":[],"LastEvaluatedKey":{"pk":{"S":"continue"}}}`
		case 4:
			require.Contains(t, input, "ExclusiveStartKey")
			// This write occurred after the upper-bound scan and matched the secondary cursor.
			return `{"Items":[{"sequence":{"N":"` + laterNumber + `"},"updated":{"S":"` + stamp + `"}}]}`
		default:
			t.Fatalf("unexpected scan %d", requests)
			return ""
		}
	})

	primary, secondary, err := d.FetchMaxCursorValues(t.Context(), stream)
	require.NoError(t, err)
	require.Equal(t, oldNumber, primary)
	require.Nil(t, secondary)
	entry := &types.StreamState{Namespace: stream.Namespace(), Stream: stream.Name()}
	entry.State.Store("sequence", primary)
	entry.State.Store("updated", secondary)
	entry.HoldsValue.Store(true)
	state := &types.State{
		Type: types.StreamType, Version: constants.LatestStateVersion, RWMutex: &sync.RWMutex{},
		Streams: []*types.StreamState{entry},
	}
	serialized, err := json.Marshal(state)
	require.NoError(t, err)
	reloaded := &types.State{RWMutex: &sync.RWMutex{}}
	require.NoError(t, json.Unmarshal(serialized, reloaded))
	d.SetupState(reloaded)

	var records []map[string]any
	primary, secondary, err = d.ReadIncrementalChanges(t.Context(), stream, func(_ context.Context, record map[string]any, _ int64) error {
		records = append(records, record)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, mustDecimal(t, laterNumber), records[0]["sequence"])
	require.Equal(t, newNumber, primary)
	require.Equal(t, stamp, secondary)
}

func TestCompositeRecordIDHandlesDelimiters(t *testing.T) {
	stream := &types.ConfiguredStream{Stream: types.NewStream("table", namespace, nil).WithPrimaryKey("pk", "sk")}
	d := DynamoDB{}
	first := map[string]any{"pk": "a|b", "sk": "c"}
	second := map[string]any{"pk": "a", "sk": "b|c"}
	require.NotEqual(t, d.RecordID(stream, first), d.RecordID(stream, second))
	require.Equal(t, d.RecordID(stream, first), d.RecordID(stream, map[string]any{"sk": "c", "pk": "a|b"}))
}

func TestParallelChunkUsesConsistentReads(t *testing.T) {
	d := DynamoDB{config: &Config{ConsistentRead: true}}
	d.client = testClient(t, func(operation string, input map[string]any) string {
		require.Equal(t, "DynamoDB_20120810.Scan", operation)
		require.Equal(t, float64(1), input["Segment"])
		require.Equal(t, float64(3), input["TotalSegments"])
		require.Equal(t, true, input["ConsistentRead"])
		return `{"Items":[{"id":{"S":"source"}}]}`
	})
	stream := &types.ConfiguredStream{Stream: types.NewStream("table", namespace, nil)}
	var rows int
	err := d.ChunkIterator(t.Context(), stream, types.Chunk{Min: float64(1), Max: float64(3)}, func(_ context.Context, record map[string]any, _ int64) error {
		require.Equal(t, "source", record["id"])
		rows++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, rows)
}

func TestBackfillFiltersBeforeSelectingColumns(t *testing.T) {
	d := DynamoDB{config: &Config{}}
	d.client = testClient(t, func(operation string, _ map[string]any) string {
		require.Equal(t, "DynamoDB_20120810.Scan", operation)
		return `{"Items":[{"id":{"S":"match"},"status":{"S":"in stock"}},{"id":{"S":"unmatched"},"status":{"S":"sold"}},{"id":{"S":"missing"}}]}`
	})
	source := types.NewStream("table", namespace, nil)
	source.UpsertField("id", types.String, false, false)
	source.UpsertField("status", types.String, true, false)
	stream := &types.ConfiguredStream{
		Stream: source,
		StreamMetadata: types.StreamMetadata{
			Filter:          `status = "in stock"`,
			SelectedColumns: &types.SelectedColumns{Columns: []string{"id"}},
		},
	}

	var rows []map[string]any
	selectColumns := stream.RetainSelectedColumns()
	err := d.ChunkIterator(t.Context(), stream, types.Chunk{Min: 0, Max: 1}, func(_ context.Context, record map[string]any, _ int64) error {
		rows = append(rows, selectColumns(record))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"id": "match"}}, rows)
}
