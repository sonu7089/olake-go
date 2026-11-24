package driver

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// convertItem turns a DynamoDB item into a plain record. Attribute names are kept
// verbatim; values map to the Go types the shared type resolver understands.
func convertItem(item map[string]ddbtypes.AttributeValue) (map[string]any, error) {
	record := make(map[string]any, len(item))
	for name, av := range item {
		value, err := convertValue(av)
		if err != nil {
			return nil, fmt.Errorf("attribute %s: %w", name, err)
		}
		record[name] = value
	}
	return record, nil
}

func convertValue(av ddbtypes.AttributeValue) (any, error) {
	switch v := av.(type) {
	case *ddbtypes.AttributeValueMemberS:
		return v.Value, nil
	case *ddbtypes.AttributeValueMemberN:
		return convertNumber(v.Value)
	case *ddbtypes.AttributeValueMemberBOOL:
		return v.Value, nil
	case *ddbtypes.AttributeValueMemberNULL:
		return nil, nil //nolint:nilnil // DynamoDB NULL is a valid attribute value.
	case *ddbtypes.AttributeValueMemberB:
		return convertBinary(v.Value), nil
	case *ddbtypes.AttributeValueMemberM:
		return convertItem(v.Value)
	case *ddbtypes.AttributeValueMemberL:
		list := make([]any, 0, len(v.Value))
		for _, elem := range v.Value {
			value, err := convertValue(elem)
			if err != nil {
				return nil, err
			}
			list = append(list, value)
		}
		return list, nil
	case *ddbtypes.AttributeValueMemberSS:
		list := make([]any, 0, len(v.Value))
		for _, elem := range v.Value {
			list = append(list, elem)
		}
		return list, nil
	case *ddbtypes.AttributeValueMemberNS:
		list := make([]any, 0, len(v.Value))
		for _, elem := range v.Value {
			value, err := convertNumber(elem)
			if err != nil {
				return nil, err
			}
			list = append(list, value)
		}
		return list, nil
	case *ddbtypes.AttributeValueMemberBS:
		list := make([]any, 0, len(v.Value))
		for _, elem := range v.Value {
			list = append(list, convertBinary(elem))
		}
		return list, nil
	default:
		return nil, fmt.Errorf("unsupported attribute value type %T", av)
	}
}

// convertNumber keeps DynamoDB's 38-digit precision: integers that fit an int64 stay
// integers and representable values become exact decimals. Keep other numbers in
// their original form until the destination converts them to its numeric type.
func convertNumber(s string) (any, error) {
	if !strings.ContainsAny(s, ".eE") {
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i, nil
		}
	}
	if d, err := typeutils.ReformatDecimal(types.BigDecimal, s); err == nil {
		return d, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || !json.Valid([]byte(s)) {
		return nil, fmt.Errorf("invalid number attribute %q", s)
	}
	return json.Number(s), nil
}

func convertBinary(b []byte) any {
	if utils.IsBinarySupported() {
		return b
	}
	return base64.StdEncoding.EncodeToString(b)
}

// recordDataBytes approximates the uncompressed data size of a converted record,
// mirroring the S3 driver's metric (value sizes, keys excluded).
func recordDataBytes(record map[string]any) int64 {
	var total int64
	for _, v := range record {
		total += valueBytes(v)
	}
	return total
}

func valueBytes(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case string:
		return int64(len(x))
	case []byte:
		return int64(len(x))
	case bool:
		return 1
	case int64:
		return 8
	case float64:
		return 8
	case time.Time:
		return 8
	case map[string]any:
		var total int64
		for _, e := range x {
			total += valueBytes(e)
		}
		return total
	case []any:
		var total int64
		for _, e := range x {
			total += valueBytes(e)
		}
		return total
	default:
		return int64(len(fmt.Sprintf("%v", x)))
	}
}
