package typeutils

import (
	"context"
	"fmt"
	"runtime"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// parsedCondition holds a fully-typed condition
type parsedCondition struct {
	column   string
	operator string
	dataType types.DataType
	value    any
}

// FilterRecords applies filtering ONLY for new filters.
// For legacy filters, records are returned unchanged.
func FilterRecords(ctx context.Context, records []types.RawRecord, filter types.FilterConfig, isLegacy bool, schema any, resolve func(string) string, keepDeletesThroughFilter bool) ([]types.RawRecord, error) {
	if len(filter.Conditions) == 0 {
		return records, nil
	}
	if isLegacy {
		// Legacy filters are pushed down to SQL (see pkg/jdbc SQLFilter); skip re-filtering here.
		logger.Warnf("legacy filter detected, skipping filtering records")
		return records, nil
	}

	// Deletes kept through the filter pass straight through; only the other records are filtered.
	keep := make([]bool, len(records))
	toFilter := make([]int, 0, len(records))
	for i, record := range records {
		if op, _ := record.OlakeColumns[constants.OpType].(string); keepDeletesThroughFilter && op == "d" {
			keep[i] = true
		} else {
			toFilter = append(toFilter, i)
		}
	}

	if len(toFilter) > 0 {
		logger.Infof("filtering records with filter: %+v", filter)
		conditions, err := parseConditions(filter, resolve, func(column string) (types.DataType, error) {
			return getFilterColumnDataType(column, schema)
		})
		if err != nil {
			return nil, err
		}
		err = utils.Concurrent(ctx, toFilter, runtime.GOMAXPROCS(0)*16, func(_ context.Context, idx int, _ int) error {
			keep[idx] = matches(records[idx], conditions, filter.LogicalOperator)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	filtered := make([]types.RawRecord, 0, len(records))
	for i, record := range records {
		if keep[i] {
			filtered = append(filtered, record)
		}
	}
	return filtered, nil
}

// NewSourceRecordFilter prepares a predicate over source attribute names and values.
// It can run before column selection or conversion to a destination schema.
func NewSourceRecordFilter(filter types.FilterConfig, schema *types.TypeSchema) (func(map[string]any) (bool, error), error) {
	if len(filter.Conditions) == 0 {
		return func(map[string]any) (bool, error) { return true, nil }, nil
	}
	conditions, err := parseConditions(filter, func(column string) string { return column }, schema.GetType)
	if err != nil {
		return nil, err
	}
	isOr := strings.EqualFold(strings.TrimSpace(filter.LogicalOperator), "OR")
	return func(record map[string]any) (bool, error) {
		for _, condition := range conditions {
			value := record[condition.column]
			if value != nil {
				converted, err := ParseFilterValue(condition.dataType, value)
				if err != nil && err != ErrNullValue {
					return false, fmt.Errorf("failed to parse source value for filter column %q: %w", condition.column, err)
				}
				value = converted
			}
			if evaluate(value, condition.value, condition.operator) == isOr {
				return isOr, nil
			}
		}
		return !isOr, nil
	}, nil
}

func parseConditions(filter types.FilterConfig, resolve func(string) string, getType func(string) (types.DataType, error)) ([]parsedCondition, error) {
	conditions := make([]parsedCondition, 0, len(filter.Conditions))
	for _, condition := range filter.Conditions {
		column := resolve(condition.Column)
		dataType, err := getType(column)
		if err != nil {
			return nil, err
		}
		value, err := ParseFilterValue(dataType, condition.Value)
		if err != nil && err != ErrNullValue {
			return nil, fmt.Errorf("failed to parse filter value for column [%s]: %s", column, err)
		}
		conditions = append(conditions, parsedCondition{column: column, operator: condition.Operator, dataType: dataType, value: value})
	}
	return conditions, nil
}

func getFilterColumnDataType(column string, schema any) (types.DataType, error) {
	switch s := schema.(type) {
	case Fields: // parquet
		field, found := s[column]
		if !found {
			return types.Unknown, fmt.Errorf("filter column [%s] missing from schema", column)
		}
		return field.getType(), nil

	case map[string]string: // iceberg
		icebergType, found := s[column]
		if !found {
			return types.Unknown, fmt.Errorf("filter column [%s] missing from schema", column)
		}
		return types.IcebergTypeToDatatype(icebergType), nil

	default:
		return types.Unknown, fmt.Errorf("unsupported schema type for filtering: %T", schema)
	}
}

// matches evaluates AND / OR logic from filter conditions
func matches(record types.RawRecord, conditions []parsedCondition, logicalOp string) bool {
	isOr := strings.EqualFold(strings.TrimSpace(logicalOp), "OR")
	for _, c := range conditions {
		if evaluate(record.Data[c.column], c.value, c.operator) == isOr {
			return isOr
		}
	}
	return !isOr
}

// evaluate compares values using typeutils.Compare from filter conditions
func evaluate(recordVal, filterVal any, operator string) bool {
	if recordVal == nil || filterVal == nil {
		return (operator == "=" && recordVal == filterVal) || (operator == "!=" && recordVal != filterVal)
	}

	cmp := Compare(recordVal, filterVal)

	return (operator == "=" && cmp == 0) || (operator == "!=" && cmp != 0) || (operator == ">" && cmp > 0) || (operator == ">=" && cmp >= 0) || (operator == "<" && cmp < 0) || (operator == "<=" && cmp <= 0)
}
