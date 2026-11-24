package driver

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// namespace is the stream namespace for every DynamoDB table, like "topics" for Kafka.
const namespace = string(constants.DynamoDB)

// DynamoDB represents the DynamoDB source driver
type DynamoDB struct {
	client       *dynamodb.Client
	config       *Config
	state        *types.State
	tablePattern *regexp.Regexp
	tables       sync.Map
}

// tableInfo caches what DescribeTable reported, so backfill and discover do not repeat it.
type tableInfo struct {
	primaryKey []string
	keyTypes   map[string]ddbtypes.ScalarAttributeType
	itemCount  int64
}

// GetConfigRef returns a reference to the config struct
func (d *DynamoDB) GetConfigRef() abstract.Config {
	d.config = &Config{}
	return d.config
}

// Spec returns the configuration specification
func (d *DynamoDB) Spec() any {
	return Config{}
}

// Type returns the driver type identifier
func (d *DynamoDB) Type() string {
	return namespace
}

// Setup initializes the DynamoDB client and validates the configuration
func (d *DynamoDB) Setup(ctx context.Context) error {
	if err := d.config.Validate(); err != nil {
		return fmt.Errorf("failed to validate config: %w", err)
	}
	d.tablePattern = nil
	d.tables.Clear()

	if d.config.TablePattern != "" {
		d.tablePattern = regexp.MustCompile(d.config.TablePattern)
		logger.Infof("Using table pattern filter: %s", d.config.TablePattern)
	}

	region := d.config.Region
	if region == "" {
		// A custom endpoint (DynamoDB Local) still signs requests with a region.
		region = "us-east-1"
	}

	loadOptions := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	switch {
	case d.config.AccessKeyID != "":
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(d.config.AccessKeyID, d.config.SecretAccessKey, d.config.SessionToken)))
	case d.config.Endpoint != "":
		// DynamoDB Local accepts any credentials, but the SDK must have some to sign with.
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("local", "local", "")))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	d.client = dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if d.config.Endpoint != "" {
			o.BaseEndpoint = aws.String(d.config.Endpoint)
		}
	})

	logger.Infof("Testing connection to DynamoDB (region: %s)", region)
	if _, err := d.client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(1)}); err != nil {
		return fmt.Errorf("failed to connect to DynamoDB: %w", err)
	}

	logger.Info("Successfully connected to DynamoDB")
	return nil
}

// SetupState sets the state reference for tracking sync progress
func (d *DynamoDB) SetupState(state *types.State) {
	d.state = state
}

// StateType returns the type of state management this driver uses
func (d *DynamoDB) StateType() types.StateType {
	return types.StreamType
}

// MaxConnections returns the maximum number of concurrent connections
func (d *DynamoDB) MaxConnections() int {
	return d.config.MaxThreads
}

// MaxRetries returns the maximum number of retry attempts
func (d *DynamoDB) MaxRetries() int {
	return d.config.RetryCount
}

// GetStreamNames discovers the tables matching the configuration
func (d *DynamoDB) GetStreamNames(ctx context.Context) ([]types.StreamID, error) {
	logger.Info("Discovering DynamoDB tables")

	var streamNames []types.StreamID
	paginator := dynamodb.NewListTablesPaginator(d.client, &dynamodb.ListTablesInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list tables: %w", err)
		}
		for _, name := range page.TableNames {
			if d.tablePattern != nil && !d.tablePattern.MatchString(name) {
				logger.Debugf("Skipping table %s (does not match pattern)", name)
				continue
			}
			streamNames = append(streamNames, types.StreamID{Namespace: namespace, Name: name})
		}
	}

	logger.Infof("Discovered %d DynamoDB tables", len(streamNames))
	return streamNames, nil
}

// describeTable fetches and caches the table's key schema and item count.
func (d *DynamoDB) describeTable(ctx context.Context, name string) (*tableInfo, error) {
	if info, ok := d.tables.Load(name); ok {
		return info.(*tableInfo), nil
	}

	out, err := d.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
	if err != nil {
		return nil, fmt.Errorf("failed to describe table %s: %w", name, err)
	}

	info := &tableInfo{itemCount: aws.ToInt64(out.Table.ItemCount), keyTypes: make(map[string]ddbtypes.ScalarAttributeType)}
	for _, key := range out.Table.KeySchema {
		info.primaryKey = append(info.primaryKey, aws.ToString(key.AttributeName))
	}
	for _, def := range out.Table.AttributeDefinitions {
		info.keyTypes[aws.ToString(def.AttributeName)] = def.AttributeType
	}
	d.tables.Store(name, info)
	return info, nil
}

// discover samples tables in buckets through ProduceSampledSchema
var _ abstract.SampledSchemaProducer = (*DynamoDB)(nil)

func (d *DynamoDB) ProduceSchema(ctx context.Context, streamID types.StreamID) (*types.Stream, error) {
	buckets := abstract.DiscoverSampleBuckets
	return d.ProduceSampledSchema(ctx, streamID, buckets[len(buckets)-1])
}

// ProduceSampledSchema builds the table's schema from at most limit scanned items.
// DynamoDB is schemaless, so the sample is the only schema evidence; key attributes
// are always present with their declared types even when the table is empty.
func (d *DynamoDB) ProduceSampledSchema(ctx context.Context, streamID types.StreamID, limit int) (*types.Stream, error) {
	if limit < 1 || limit > math.MaxInt32 {
		return nil, fmt.Errorf("invalid schema sample size %d", limit)
	}
	logger.Infof("Producing schema for table %s from up to %d items", streamID.Name, limit)

	info, err := d.describeTable(ctx, streamID.Name)
	if err != nil {
		return nil, err
	}

	stream := types.NewStream(streamID.Name, streamID.Namespace, nil)
	stream.WithPrimaryKey(info.primaryKey...)

	input := dynamodb.ScanInput{
		TableName:      aws.String(streamID.Name),
		ConsistentRead: aws.Bool(d.config.ConsistentRead),
	}
	var sample typeutils.SchemaSample
	remaining := int32(limit)
	cursorTypes := make(map[string]string)
	for remaining > 0 {
		input.Limit = aws.Int32(remaining)
		page, err := d.client.Scan(ctx, &input)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table %s for schema sampling: %w", streamID.Name, err)
		}
		for _, item := range page.Items {
			record, err := convertItem(item)
			if err != nil {
				return nil, fmt.Errorf("failed to convert sampled item of table %s: %w", streamID.Name, err)
			}
			sample.Add(record)
			remaining--
			for field, value := range item {
				if _, null := value.(*ddbtypes.AttributeValueMemberNULL); null {
					continue
				}
				kind := cursorAttributeType(value)
				if previous, known := cursorTypes[field]; known && previous != kind {
					kind = ""
				}
				cursorTypes[field] = kind
			}
		}
		if len(page.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = page.LastEvaluatedKey
	}
	sample.Apply(stream)

	// Key attributes are guaranteed on every item; declare them even for an empty table.
	for _, key := range info.primaryKey {
		if _, err := stream.Schema.GetType(key); err != nil {
			keyType := types.String
			switch info.keyTypes[key] {
			case ddbtypes.ScalarAttributeTypeS:
				cursorTypes[key] = "S"
			case ddbtypes.ScalarAttributeTypeN:
				keyType = types.Int64
				cursorTypes[key] = "N"
			case ddbtypes.ScalarAttributeTypeB:
				if utils.IsBinarySupported() {
					keyType = types.Binary
				}
			}
			stream.UpsertField(key, keyType, false, false)
		}
	}

	for column, kind := range cursorTypes {
		detected, err := stream.Schema.GetType(column)
		if err == nil && kind != "" && kind == cursorType(detected) && !strings.Contains(column, ":") {
			stream.WithCursorField(column)
		}
	}
	stream.WithSyncMode(types.FULLREFRESH)
	if stream.AvailableCursorFields.Len() > 0 {
		stream.WithSyncMode(types.INCREMENTAL)
	}
	stream.SyncMode = types.FULLREFRESH
	return stream, nil
}

// CloseConnection closes the DynamoDB client connection
func (d *DynamoDB) CloseConnection() {
	logger.Info("Closing DynamoDB connection")
	// The DynamoDB client doesn't require explicit cleanup
}
