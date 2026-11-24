package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
)

// ChangeStreamConfig returns the change stream configuration for DynamoDB
func (d *DynamoDB) ChangeStreamConfig() (bool, bool, bool) {
	return false, false, false
}

// CDCSupported returns false: DynamoDB Streams retain changes for only 24 hours,
// which cannot guarantee gap-free change capture across syncs.
func (d *DynamoDB) CDCSupported() bool {
	return false
}

// PreCDC is not supported for DynamoDB
func (d *DynamoDB) PreCDC(_ context.Context, _ []types.StreamInterface) error {
	return errs.Precondition(errs.UnsupportedFeature, codeCDCUnsupported,
		fmt.Errorf("CDC is not supported for DynamoDB source"))
}

// StreamChanges is not supported for DynamoDB
func (d *DynamoDB) StreamChanges(_ context.Context, _ int, _ map[string]any, _ abstract.CDCMsgFn) (any, error) {
	return nil, errs.Precondition(errs.UnsupportedFeature, codeCDCUnsupported,
		fmt.Errorf("CDC is not supported for DynamoDB source"))
}

// PostCDC is not supported for DynamoDB
func (d *DynamoDB) PostCDC(_ context.Context, _ int) error {
	return errs.Precondition(errs.UnsupportedFeature, codeCDCUnsupported,
		fmt.Errorf("CDC is not supported for DynamoDB source"))
}
