//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"aws_stuff/internal/repository"
	"aws_stuff/internal/service"
	"aws_stuff/pkg/models"

	"go.uber.org/zap"
)

func requireIntegration(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("set RUN_INTEGRATION_TESTS=1 to run integration tests")
	}
}

func newLocalstackConfig(t *testing.T) awsv2.Config {
	region := getenv("AWS_REGION", "us-east-1")
	endpoint := getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")

	resolver := awsv2.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (awsv2.Endpoint, error) {
			return awsv2.Endpoint{
				URL:               endpoint,
				HostnameImmutable: true,
			}, nil
		},
	)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithEndpointResolverWithOptions(resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	return cfg
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func Test_EndToEnd(t *testing.T) {
	requireIntegration(t)
	ctx := context.Background()
	awsCfg := newLocalstackConfig(t)

	// Clients
	db := dynamodb.NewFromConfig(awsCfg)
	streams := dynamodbstreams.NewFromConfig(awsCfg)
	sqsClient := sqs.NewFromConfig(awsCfg)
	snsClient := sns.NewFromConfig(awsCfg)
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) { o.UsePathStyle = true })
	cwClient := cloudwatch.NewFromConfig(awsCfg)

	// Resources
	table := fmt.Sprintf("it-items-%d", time.Now().UnixNano())
	replicaTable := table + "-replica"
	bucket := fmt.Sprintf("it-bucket-%d", time.Now().UnixNano())
	queue := fmt.Sprintf("it-queue-%d", time.Now().UnixNano())
	forwardQueue := queue + "-fwd"
	topic := fmt.Sprintf("arn:aws:sns:us-east-1:000000000000:it-topic-%d", time.Now().UnixNano())

	createDynamoTable(t, db, table, true)
	createDynamoTable(t, db, replicaTable, false)
	createBucket(t, s3Client, bucket)
	queueURL := createQueue(t, sqsClient, queue)
	fwdQueueURL := createQueue(t, sqsClient, forwardQueue)
	topicARN := createTopic(t, snsClient, topic)
	subscribeQueueToTopic(t, sqsClient, snsClient, queueURL, topicARN)

	// Processor service
	log, _ := zap.NewDevelopment()
	dynamoRepo := repository.NewDynamoRepo(db, table, replicaTable)
	sqsRepo := repository.NewSQSRepo(sqsClient)
	snsRepo := repository.NewSNSRepo(snsClient)
	s3Repo := repository.NewS3Repo(s3Client)
	cwRepo := repository.NewCloudWatchRepo(cwClient)
	proc := service.NewProcessorService(log, dynamoRepo, sqsRepo, snsRepo, s3Repo, cwRepo)

	t.Run("SQS -> DynamoDB", func(t *testing.T) {
		body := `{"id":"1","type":"demo","content":"hello"}`
		_, err := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: &queueURL, MessageBody: &body})
		if err != nil {
			t.Fatalf("send: %v", err)
		}
		msgs := receiveOnce(t, sqsClient, queueURL)
		if len(msgs) == 0 {
			t.Fatalf("no messages")
		}
		if err := proc.HandleSQS(ctx, awsv2.ToString(msgs[0].Body)); err != nil {
			t.Fatalf("handle: %v", err)
		}
	})

	t.Run("SNS -> SQS forward", func(t *testing.T) {
		msg := "sns-message"
		_, err := snsClient.Publish(ctx, &sns.PublishInput{TopicArn: &topicARN, Message: &msg})
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
		msgs := receiveOnce(t, sqsClient, queueURL)
		if len(msgs) == 0 {
			t.Fatalf("no sns msgs")
		}
		var env struct {
			Message string `json:"Message"`
		}
		_ = json.Unmarshal([]byte(awsv2.ToString(msgs[0].Body)), &env)
		if err := proc.HandleSNS(ctx, env.Message, fwdQueueURL); err != nil {
			t.Fatalf("forward: %v", err)
		}
		fwd := receiveOnce(t, sqsClient, fwdQueueURL)
		if len(fwd) == 0 || awsv2.ToString(fwd[0].Body) != msg {
			t.Fatalf("forward mismatch: %#v", fwd)
		}
	})

	t.Run("S3 -> SQS -> DynamoDB", func(t *testing.T) {
		// Configure bucket notification to queue
		setBucketQueueNotification(t, s3Client, bucket, queueARNFromURL(queueURL))
		// Put object
		key := "test.txt"
		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: strings.NewReader("content")})
		if err != nil {
			t.Fatalf("put: %v", err)
		}
		// Receive event and process
		msgs := receiveOnce(t, sqsClient, queueURL)
		if len(msgs) == 0 {
			t.Fatalf("no s3 event")
		}
		var ev struct {
			Records []struct {
				S3 struct {
					Bucket struct {
						Name string `json:"name"`
					} `json:"bucket"`
					Object struct {
						Key string `json:"key"`
					} `json:"object"`
				} `json:"s3"`
			} `json:"Records"`
		}
		_ = json.Unmarshal([]byte(awsv2.ToString(msgs[0].Body)), &ev)
		if len(ev.Records) == 0 {
			t.Fatalf("no records")
		}
		if err := proc.HandleS3Object(ctx, ev.Records[0].S3.Bucket.Name, ev.Records[0].S3.Object.Key); err != nil {
			t.Fatalf("handle s3: %v", err)
		}
	})

	t.Run("DynamoDB Streams replicate", func(t *testing.T) {
		// Insert an item to source table to generate a stream record
		_, err := db.PutItem(ctx, &dynamodb.PutItemInput{TableName: &table, Item: map[string]dynamodbtypes.AttributeValue{
			"pk":      &dynamodbtypes.AttributeValueMemberS{Value: "pk-1"},
			"sk":      &dynamodbtypes.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339Nano)},
			"payload": &dynamodbtypes.AttributeValueMemberS{Value: "p"},
		}})
		if err != nil {
			t.Fatalf("put: %v", err)
		}
		// Read from streams and replicate
		desc, err := streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: getStreamARN(t, db, table)})
		if err != nil || desc.StreamDescription == nil || len(desc.StreamDescription.Shards) == 0 {
			t.Fatalf("describe: %v", err)
		}
		itOut, err := streams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
			StreamArn:         desc.StreamDescription.StreamArn,
			ShardId:           desc.StreamDescription.Shards[0].ShardId,
			ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
		})
		if err != nil {
			t.Fatalf("iterator: %v", err)
		}
		it := itOut.ShardIterator
		proc2 := proc
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			rec, _ := streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: it})
			if rec != nil && len(rec.Records) > 0 {
				for _, r := range rec.Records {
					if r.Dynamodb == nil || r.Dynamodb.NewImage == nil {
						continue
					}
					item := modelsItemFromImage(r.Dynamodb.NewImage)
					_ = proc2.HandleStreamRecord(ctx, item)
				}
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		// Basic check: put to replica table
		out, err := db.Scan(ctx, &dynamodb.ScanInput{TableName: &replicaTable, Select: dynamodbtypes.SelectCount})
		if err != nil || out.Count == 0 {
			t.Fatalf("replica count: %v %v", err, out)
		}
	})
}

func receiveOnce(t *testing.T, client *sqs.Client, queueURL string) []sqstypes.Message {
	ctx := context.Background()
	out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     5,
	})
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	return out.Messages
}

func createDynamoTable(t *testing.T, db *dynamodb.Client, name string, withStream bool) {
	ctx := context.Background()
	attr := []dynamodbtypes.AttributeDefinition{
		{AttributeName: awsv2.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		{AttributeName: awsv2.String("sk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
	}
	key := []dynamodbtypes.KeySchemaElement{
		{AttributeName: awsv2.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
		{AttributeName: awsv2.String("sk"), KeyType: dynamodbtypes.KeyTypeRange},
	}

	var capacity int64 = 5
	in := &dynamodb.CreateTableInput{
		TableName:            &name,
		AttributeDefinitions: attr,
		KeySchema:            key,
		ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  &capacity,
			WriteCapacityUnits: &capacity,
		},
	}

	streamEnable := true
	if withStream {
		in.StreamSpecification = &dynamodbtypes.StreamSpecification{
			StreamEnabled:  &streamEnable,
			StreamViewType: dynamodbtypes.StreamViewTypeNewImage,
		}
	}
	_, _ = db.CreateTable(ctx, in)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		out, _ := db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &name})
		if out != nil && out.Table != nil && out.Table.TableStatus == dynamodbtypes.TableStatusActive {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("table not active: %s", name)
}

func getStreamARN(t *testing.T, db *dynamodb.Client, table string) *string {
	ctx := context.Background()
	out, err := db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &table})
	if err != nil || out.Table == nil || out.Table.LatestStreamArn == nil {
		t.Fatalf("stream arn: %v", err)
	}
	return out.Table.LatestStreamArn
}

func createBucket(t *testing.T, s3c *s3.Client, name string) {
	ctx := context.Background()
	_, _ = s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &name})
}

func createQueue(t *testing.T, sqsClient *sqs.Client, name string) string {
	ctx := context.Background()
	out, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &name})
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	return awsv2.ToString(out.QueueUrl)
}

func queueARNFromURL(url string) string {
	// LocalStack pattern: http://localhost:4566/000000000000/queueName
	parts := strings.Split(url, "/")
	name := parts[len(parts)-1]
	return fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:%s", name)
}

func setBucketQueueNotification(t *testing.T, s3c *s3.Client, bucket string, queueARN string) {
	ctx := context.Background()
	_, err := s3c.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
		Bucket: &bucket,
		NotificationConfiguration: &s3types.NotificationConfiguration{
			QueueConfigurations: []s3types.QueueConfiguration{{
				QueueArn: awsv2.String(queueARN),
				Events: []s3types.Event{
					s3types.Event("s3:ObjectCreated:*"),
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("set notif: %v", err)
	}
}

func createTopic(t *testing.T, snsClient *sns.Client, arn string) string {
	ctx := context.Background()
	name := arn[strings.LastIndex(arn, ":")+1:]
	out, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: &name})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	return awsv2.ToString(out.TopicArn)
}

func subscribeQueueToTopic(t *testing.T, sqsClient *sqs.Client, snsClient *sns.Client, queueURL string, topicARN string) {
	ctx := context.Background()
	attrs, _ := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	queueARN := attrs.Attributes["QueueArn"]
	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"sqs:SendMessage","Resource":"%s","Condition":{"ArnEquals":{"aws:SourceArn":"%s"}}}]}`, queueARN, topicARN)
	_, _ = sqsClient.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl:   &queueURL,
		Attributes: map[string]string{"Policy": policy},
	})
	_, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: &topicARN,
		Protocol: awsv2.String("sqs"),
		Endpoint: &queueARN,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
}

// helpers
func modelsItemFromImage(img map[string]streamstypes.AttributeValue) models.Item {
	return models.Item{
		PK:      img["pk"].(*streamstypes.AttributeValueMemberS).Value,
		SK:      img["sk"].(*streamstypes.AttributeValueMemberS).Value,
		Payload: img["payload"].(*streamstypes.AttributeValueMemberS).Value,
	}
}
