package driver

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/require"
)

func TestClassifyDynamoDBErrors(t *testing.T) {
	sourceError := &smithy.OperationError{
		ServiceID:     dynamodb.ServiceID,
		OperationName: "Scan",
		Err:           &ddbtypes.ResourceNotFoundException{},
	}
	failure := classify(sourceError)
	require.NotNil(t, failure)
	require.Equal(t, errs.ObjectNotFound, failure.Category)

	sourceError.Err = &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusTooManyRequests}},
	}
	failure = classify(sourceError)
	require.NotNil(t, failure)
	require.Equal(t, errs.ResourceExhausted, failure.Category)
	require.Equal(t, "http_429", failure.Code)

	destinationError := &smithy.OperationError{
		ServiceID:     "S3",
		OperationName: "PutObject",
		Err:           &smithy.GenericAPIError{Code: "AccessDeniedException"},
	}
	require.Nil(t, classify(destinationError))
}
