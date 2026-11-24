package driver

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go"
	"github.com/datazip-inc/olake/utils/errs"
)

// Codes for conditions this driver detects itself, where no API error exists to read.
const (
	codeCDCUnsupported      = "dynamodb.cdc_unsupported"
	codeRegionMissing       = "dynamodb.region_missing"
	codeIncompleteAuth      = "dynamodb.credentials_incomplete"
	codeTablePatternInvalid = "dynamodb.table_pattern_invalid"
)

// Registered so ReportFailure can classify without knowing which connector ran.
func init() { errs.Register("dynamodb", classify) }

// classify reads DynamoDB's API code, falling back to the HTTP status.
func classify(err error) *errs.Failure {
	// The same binary also runs AWS-backed destinations with Smithy errors.
	var operation *smithy.OperationError
	if !errors.As(err, &operation) || operation.Service() != dynamodb.ServiceID {
		return nil
	}

	var code string
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code = apiErr.ErrorCode()
		if category, ok := serviceCodeCategories[code]; ok {
			return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByVendor, Code: code}
		}
	}

	var httpErr interface{ HTTPStatusCode() int }
	if errors.As(err, &httpErr) {
		if category := categoryForStatus(httpErr.HTTPStatusCode()); category != "" {
			if code == "" {
				code = fmt.Sprintf("http_%d", httpErr.HTTPStatusCode())
			}
			return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByVendor, Code: code}
		}
	}

	if code == "" {
		return nil
	}
	return &errs.Failure{Category: errs.Unclassified, ClassifiedBy: errs.ClassifiedByDefault, Code: code}
}

var serviceCodeCategories = map[string]errs.Category{
	"ResourceNotFoundException":                errs.ObjectNotFound,
	"TableNotFoundException":                   errs.ObjectNotFound,
	"AccessDeniedException":                    errs.PermissionDenied,
	"UnrecognizedClientException":              errs.AuthFailed,
	"InvalidSignatureException":                errs.AuthFailed,
	"MissingAuthenticationToken":               errs.AuthFailed,
	"IncompleteSignature":                      errs.AuthFailed,
	"ExpiredTokenException":                    errs.AuthFailed,
	"InvalidClientTokenId":                     errs.AuthFailed,
	"ProvisionedThroughputExceededException":   errs.ResourceExhausted,
	"RequestLimitExceeded":                     errs.ResourceExhausted,
	"ThrottlingException":                      errs.ResourceExhausted,
	"TransactionConflictException":             errs.ResourceExhausted,
	"ReplicatedWriteConflictException":         errs.ResourceExhausted,
	"LimitExceededException":                   errs.ResourceExhausted,
	"ValidationException":                      errs.ConfigInvalid,
	"InternalServerError":                      errs.NetworkUnreachable,
	"ServiceUnavailable":                       errs.NetworkUnreachable,
	"ItemCollectionSizeLimitExceededException": errs.SourceReadError,
}

func categoryForStatus(status int) errs.Category {
	switch {
	case status == 401:
		return errs.AuthFailed
	case status == 403:
		return errs.PermissionDenied
	case status == 404:
		return errs.ObjectNotFound
	case status == 408:
		return errs.Timeout
	case status == 429:
		return errs.ResourceExhausted
	case status >= 500:
		return errs.NetworkUnreachable
	}
	return ""
}
