package driver

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

var _ abstract.IncrementalCursorReader = (*DynamoDB)(nil)

type scanCursor struct {
	field  string
	kind   string
	value  *string
	number *big.Rat
}

func (d *DynamoDB) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	cursors, err := scanCursors(stream)
	if err != nil {
		return nil, nil, err
	}
	if err := d.fetchMaxCursors(ctx, stream.Name(), cursors); err != nil {
		return nil, nil, err
	}
	primary, secondary := cursorCheckpoints(cursors)
	return primary, secondary, nil
}

func (d *DynamoDB) fetchMaxCursors(ctx context.Context, table string, cursors []scanCursor) error {
	input := &dynamodb.ScanInput{
		TableName:                aws.String(table),
		ConsistentRead:           aws.Bool(true),
		ExpressionAttributeNames: make(map[string]string, len(cursors)),
	}
	fields := make([]string, 0, len(cursors))
	for i, cursor := range cursors {
		field := fmt.Sprintf("#cursor%d", i)
		fields = append(fields, field)
		input.ExpressionAttributeNames[field] = cursor.field
	}
	input.ProjectionExpression = aws.String(strings.Join(fields, ", "))
	return d.scanItems(ctx, input, func(item map[string]ddbtypes.AttributeValue) error {
		for i := range cursors {
			cursor := &cursors[i]
			if _, err := cursor.advance(item[cursor.field]); err != nil {
				return fmt.Errorf("invalid cursor %s.%s: %w", table, cursor.field, err)
			}
		}
		return nil
	})
}

func (d *DynamoDB) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb abstract.BackfillMsgFn) error {
	_, _, err := d.ReadIncrementalChanges(ctx, stream, cb)
	return err
}

func (d *DynamoDB) ReadIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb abstract.BackfillMsgFn) (any, any, error) {
	cursors, err := scanCursors(stream)
	if err != nil {
		return nil, nil, err
	}

	// Capture upper bounds before reading data. Advancing to the maximum seen while
	// reading data could skip a concurrent write to a segment already scanned.
	if err := d.fetchMaxCursors(ctx, stream.Name(), cursors); err != nil {
		return nil, nil, err
	}

	input := &dynamodb.ScanInput{
		TableName:                 aws.String(stream.Name()),
		ConsistentRead:            aws.Bool(true),
		ExpressionAttributeNames:  make(map[string]string, len(cursors)),
		ExpressionAttributeValues: make(map[string]ddbtypes.AttributeValue, len(cursors)),
	}
	conditions := make([]string, 0, len(cursors))
	for i := range cursors {
		upper := cursors[i]
		cursor := scanCursor{field: upper.field, kind: upper.kind}
		var lowerBound ddbtypes.AttributeValue
		if checkpoint := d.state.GetCursor(stream.Self(), upper.field); checkpoint != nil {
			lowerBound, err = cursor.attributeValue(checkpoint)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid checkpoint for %s.%s: %w", stream.Name(), cursor.field, err)
			}
			if _, err := cursor.advance(lowerBound); err != nil {
				return nil, nil, err
			}
		}
		cursors[i] = cursor
		if upper.value == nil {
			continue
		}
		upperBound, err := upper.attributeValue(*upper.value)
		if err != nil {
			return nil, nil, err
		}
		advanced, err := cursors[i].advance(upperBound)
		if err != nil {
			return nil, nil, err
		}
		if !advanced {
			continue
		}

		field, high := fmt.Sprintf("#cursor%d", i), fmt.Sprintf(":high%d", i)
		input.ExpressionAttributeNames[field] = upper.field
		input.ExpressionAttributeValues[high] = upperBound
		condition := field + " <= " + high
		if lowerBound != nil {
			low := fmt.Sprintf(":low%d", i)
			input.ExpressionAttributeValues[low] = lowerBound
			condition = "(" + field + " > " + low + " AND " + condition + ")"
		}
		conditions = append(conditions, condition)
	}
	if len(conditions) > 0 {
		input.FilterExpression = aws.String(strings.Join(conditions, " OR "))
		if err := d.scanAndProcess(ctx, stream, input, cb); err != nil {
			return nil, nil, err
		}
	}
	primary, secondary := cursorCheckpoints(cursors)
	return primary, secondary, nil
}

func scanCursors(stream types.StreamInterface) ([]scanCursor, error) {
	primary, secondary := stream.Cursor()
	if primary == "" || primary == secondary {
		return nil, fmt.Errorf("table %s requires a primary cursor and, if set, a different secondary cursor", stream.Name())
	}

	cursors := make([]scanCursor, 0, 2)
	for _, field := range []string{primary, secondary} {
		if field == "" {
			continue
		}
		dataType, err := stream.Schema().GetType(field)
		if err != nil {
			return nil, err
		}
		kind := cursorType(dataType)
		if kind == "" || !stream.GetStream().AvailableCursorFields.Exists(field) {
			return nil, fmt.Errorf("table %s cursor %q must be a discovered Number or String attribute", stream.Name(), field)
		}
		cursors = append(cursors, scanCursor{field: field, kind: kind})
	}
	return cursors, nil
}

func cursorCheckpoints(cursors []scanCursor) (any, any) {
	var secondary any
	if len(cursors) > 1 {
		secondary = cursors[1].checkpoint()
	}
	return cursors[0].checkpoint(), secondary
}

func (c *scanCursor) checkpoint() any {
	if c.value == nil {
		return nil
	}
	return *c.value
}

func (c *scanCursor) advance(attr ddbtypes.AttributeValue) (bool, error) {
	switch v := attr.(type) {
	case *ddbtypes.AttributeValueMemberS:
		if c.kind == "S" && (c.value == nil || v.Value > *c.value) {
			c.value = &v.Value
			return true, nil
		}
	case *ddbtypes.AttributeValueMemberN:
		if c.kind != "N" {
			return false, nil
		}
		number, ok := new(big.Rat).SetString(v.Value)
		if !ok {
			return false, fmt.Errorf("invalid numeric cursor %q", v.Value)
		}
		if c.number == nil || number.Cmp(c.number) > 0 {
			c.number, c.value = number, &v.Value
			return true, nil
		}
	}
	return false, nil
}

func (c scanCursor) attributeValue(checkpoint any) (ddbtypes.AttributeValue, error) {
	if c.kind == "N" {
		// Older state may hold a JSON number. New checkpoints store exact strings.
		text := fmt.Sprint(checkpoint)
		if number, err := strconv.ParseFloat(text, 64); err != nil || math.IsInf(number, 0) || math.IsNaN(number) {
			return nil, fmt.Errorf("invalid numeric cursor %q", text)
		}
		return &ddbtypes.AttributeValueMemberN{Value: text}, nil
	}
	value, ok := checkpoint.(string)
	if !ok {
		return nil, fmt.Errorf("expected a string cursor, got %T", checkpoint)
	}
	return &ddbtypes.AttributeValueMemberS{Value: value}, nil
}

func cursorAttributeType(attr ddbtypes.AttributeValue) string {
	switch attr.(type) {
	case *ddbtypes.AttributeValueMemberS:
		return "S"
	case *ddbtypes.AttributeValueMemberN:
		return "N"
	default:
		return ""
	}
}

func cursorType(dataType types.DataType) string {
	switch dataType {
	case types.Int32, types.Int64, types.Float32, types.Float64, types.BigDecimal:
		return "N"
	case types.String, types.Timestamp, types.TimestampMilli, types.TimestampMicro, types.TimestampNano:
		return "S"
	}
	if isDecimal, _, _ := types.IsDecimal(dataType); isDecimal {
		return "N"
	}
	return ""
}
