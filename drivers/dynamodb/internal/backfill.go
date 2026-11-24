package driver

import (
	"context"
	"fmt"
	"math"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// GetOrSplitChunks splits the table into parallel scan segments
func (d *DynamoDB) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	info, err := d.describeTable(ctx, stream.Name())
	if err != nil {
		return nil, err
	}

	segments := max(d.config.MaxThreads, 1)
	logger.Infof("Backfill: scanning table %s with %d parallel segments (~%d items)", stream.Name(), segments, info.itemCount)

	// ItemCount is an AWS-maintained approximation; it drives progress stats only.
	pool.AddRecordsToSyncStats(info.itemCount)

	chunks := types.NewSet[types.Chunk]()
	for segment := range segments {
		chunks.Insert(types.Chunk{Min: segment, Max: segments})
	}
	return chunks, nil
}

// ChunkIterator scans one segment of the table, page by page
func (d *DynamoDB) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn abstract.BackfillMsgFn) error {
	segment, err := chunkInt(chunk.Min)
	if err != nil {
		return fmt.Errorf("invalid chunk segment: %w", err)
	}
	totalSegments, err := chunkInt(chunk.Max)
	if err != nil {
		return fmt.Errorf("invalid chunk total segments: %w", err)
	}
	if segment >= totalSegments {
		return fmt.Errorf("invalid scan segment %d of %d", segment, totalSegments)
	}

	input := &dynamodb.ScanInput{
		TableName:      aws.String(stream.Name()),
		ConsistentRead: aws.Bool(d.config.ConsistentRead || stream.GetSyncMode() == types.INCREMENTAL),
	}
	if totalSegments > 1 {
		input.Segment = aws.Int32(segment)
		input.TotalSegments = aws.Int32(totalSegments)
	}

	logger.Infof("Scanning segment %d/%d of table %s", segment, totalSegments, stream.Name())
	return d.scanAndProcess(ctx, stream, input, processFn)
}

func (*DynamoDB) AppliesSourceFilters() bool { return true }

// scanAndProcess pages through a Scan, converting and forwarding every item.
func (d *DynamoDB) scanAndProcess(ctx context.Context, stream types.StreamInterface, input *dynamodb.ScanInput, processFn abstract.BackfillMsgFn) error {
	filter, legacy, err := stream.GetFilter()
	if err != nil {
		return fmt.Errorf("failed to parse filter for %s: %w", stream.Name(), err)
	}
	if legacy {
		for i, condition := range filter.Conditions {
			value, quoted := condition.Value.(string)
			if quoted && len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
				filter.Conditions[i].Value = value[1 : len(value)-1]
			}
		}
	}
	matches, err := typeutils.NewSourceRecordFilter(filter, stream.Schema())
	if err != nil {
		return fmt.Errorf("invalid filter for %s: %w", stream.Name(), err)
	}

	return d.scanItems(ctx, input, func(item map[string]ddbtypes.AttributeValue) error {
		record, err := convertItem(item)
		if err != nil {
			return fmt.Errorf("failed to convert item of table %s: %w", aws.ToString(input.TableName), err)
		}
		matched, err := matches(record)
		if err != nil {
			return err
		}
		if !matched {
			return nil
		}
		return processFn(ctx, record, recordDataBytes(record))
	})
}

func (d *DynamoDB) scanItems(ctx context.Context, input *dynamodb.ScanInput, processItem func(map[string]ddbtypes.AttributeValue) error) error {
	paginator := dynamodb.NewScanPaginator(d.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to scan table %s: %w", aws.ToString(input.TableName), err)
		}
		for _, item := range page.Items {
			if err := processItem(item); err != nil {
				return err
			}
		}
	}
	return nil
}

// chunkInt reads a chunk bound that JSON state round-trips as float64.
func chunkInt(v any) (int32, error) {
	var number float64
	switch n := v.(type) {
	case int:
		number = float64(n)
	case int32:
		number = float64(n)
	case int64:
		number = float64(n)
	case float64:
		number = n
	default:
		return 0, fmt.Errorf("expected a number, got %T", v)
	}
	if number < 0 || number > maxScanSegments || number != math.Trunc(number) {
		return 0, fmt.Errorf("expected a scan bound between 0 and %d, got %v", maxScanSegments, v)
	}
	return int32(number), nil
}

func (d *DynamoDB) RecordID(stream types.StreamInterface, record map[string]any) string {
	keys := stream.GetStream().SourceDefinedPrimaryKey.Array()
	if len(keys) < 2 {
		return utils.GetKeysHash(record, keys...)
	}

	// Length-prefix each value so composite keys can contain the shared delimiter.
	keyValues := make(map[string]any, len(keys))
	for _, key := range keys {
		value := fmt.Sprint(record[key])
		keyValues[key] = fmt.Sprintf("%d:%s", len(value), value)
	}
	return utils.GetKeysHash(keyValues, keys...)
}
