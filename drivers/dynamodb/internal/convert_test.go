package driver

import (
	"encoding/json"
	"testing"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertItem(t *testing.T) {
	item := map[string]ddbtypes.AttributeValue{
		"pk":      &ddbtypes.AttributeValueMemberS{Value: "user#1"},
		"count":   &ddbtypes.AttributeValueMemberN{Value: "42"},
		"price":   &ddbtypes.AttributeValueMemberN{Value: "123.45678901234567890123456789012345678"},
		"tiny":    &ddbtypes.AttributeValueMemberN{Value: "1e-100"},
		"active":  &ddbtypes.AttributeValueMemberBOOL{Value: true},
		"missing": &ddbtypes.AttributeValueMemberNULL{Value: true},
		"tags":    &ddbtypes.AttributeValueMemberSS{Value: []string{"a", "b"}},
		"scores":  &ddbtypes.AttributeValueMemberNS{Value: []string{"1", "2.5"}},
		"nested": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
			"city": &ddbtypes.AttributeValueMemberS{Value: "Indore"},
			"zip":  &ddbtypes.AttributeValueMemberN{Value: "452001"},
		}},
		"list": &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{
			&ddbtypes.AttributeValueMemberS{Value: "x"},
			&ddbtypes.AttributeValueMemberN{Value: "7"},
		}},
	}

	record, err := convertItem(item)
	require.NoError(t, err)

	assert.Equal(t, "user#1", record["pk"])
	assert.Equal(t, int64(42), record["count"])
	assert.Equal(t, true, record["active"])
	assert.Nil(t, record["missing"])
	assert.Equal(t, []any{"a", "b"}, record["tags"])
	assert.Equal(t, []any{int64(1), mustDecimal(t, "2.5")}, record["scores"])
	assert.Equal(t, map[string]any{"city": "Indore", "zip": int64(452001)}, record["nested"])
	assert.Equal(t, []any{"x", int64(7)}, record["list"])

	// 38-digit precision survives as an exact decimal
	decimal, ok := record["price"].(typeutils.Decimal)
	require.True(t, ok, "expected exact decimal, got %T", record["price"])
	assert.Equal(t, "123.45678901234567890123456789012345678", decimal.String())

	assert.Equal(t, json.Number("1e-100"), record["tiny"])
}

func TestConvertNumber(t *testing.T) {
	tests := []struct {
		input string
		want  any
	}{
		{"0", int64(0)},
		{"-17", int64(-17)},
		{"9223372036854775807", int64(9223372036854775807)},
		{"9223372036854775808", mustDecimal(t, "9223372036854775808")},
		{"2.5", mustDecimal(t, "2.5")},
		{"1e3", mustDecimal(t, "1000")},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := convertNumber(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	_, err := convertNumber("not-a-number")
	require.Error(t, err)
}

func TestConvertValueUnsupported(t *testing.T) {
	_, err := convertValue(nil)
	require.Error(t, err)
}

func TestCursorAttributeValue(t *testing.T) {
	t.Run("integer cursor", func(t *testing.T) {
		av, err := (scanCursor{kind: "N"}).attributeValue(int64(100))
		require.NoError(t, err)
		n, ok := av.(*ddbtypes.AttributeValueMemberN)
		require.True(t, ok)
		assert.Equal(t, "100", n.Value)
	})

	t.Run("float cursor from state", func(t *testing.T) {
		av, err := (scanCursor{kind: "N"}).attributeValue(float64(100))
		require.NoError(t, err)
		n, ok := av.(*ddbtypes.AttributeValueMemberN)
		require.True(t, ok)
		assert.Equal(t, "100", n.Value)
	})

	t.Run("string cursor", func(t *testing.T) {
		av, err := (scanCursor{kind: "S"}).attributeValue("2026-01-01T00:00:00Z")
		require.NoError(t, err)
		s, ok := av.(*ddbtypes.AttributeValueMemberS)
		require.True(t, ok)
		assert.Equal(t, "2026-01-01T00:00:00Z", s.Value)
	})

	t.Run("invalid numeric cursor", func(t *testing.T) {
		_, err := (scanCursor{kind: "N"}).attributeValue("abc")
		require.Error(t, err)
	})
}

func TestChunkInt(t *testing.T) {
	for _, v := range []any{3, int32(3), int64(3), float64(3)} {
		n, err := chunkInt(v)
		require.NoError(t, err)
		assert.Equal(t, int32(3), n)
	}
	_, err := chunkInt("3")
	require.Error(t, err)
	_, err = chunkInt(1.5)
	require.Error(t, err)
}

func mustDecimal(t *testing.T, s string) typeutils.Decimal {
	t.Helper()
	d, err := typeutils.ReformatDecimal(types.BigDecimal, s)
	require.NoError(t, err)
	return d
}
