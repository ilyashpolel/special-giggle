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

	awsv1 "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"

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

func newLocalstackSession(t *testing.T) (*session.Session, *awsv1.Config) {
	region := getenv("AWS_REGION", "us-east-1")
	endpoint := getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")
	cfg := &awsv1.Config{
		Region:           awsv1.String(region),
		Endpoint:         awsv1.String(endpoint),
		S3ForcePathStyle: awsv1.Bool(true),
		Credentials:      credentials.NewStaticCredentials("test", "test", ""),
	}
	sess, err := session.NewSession(cfg)
	if err != nil {
		t.Fatalf("aws session: %v", err)
	}
	return sess, cfg
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
	sess, baseCfg := newLocalstackSession(t)

	// Clients
	db := dynamodb.New(sess, baseCfg)
	streams := dynamodbstreams.New(sess, baseCfg)
	sqsClient := sqs.New(sess, baseCfg)
	snsClient := sns.New(sess, baseCfg)
	s3Client := s3.New(sess, baseCfg)
	cwClient := cloudwatch.New(sess, baseCfg)

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
		_, err := sqsClient.SendMessage(&sqs.SendMessageInput{QueueUrl: &queueURL, MessageBody: &body})
		if err != nil {
			t.Fatalf("send: %v", err)
		}
		msgs := receiveOnce(t, sqsClient, queueURL)
		if len(msgs) == 0 {
			t.Fatalf("no messages")
		}
		if err := proc.HandleSQS(ctx, awsv1.StringValue(msgs[0].Body)); err != nil {
			t.Fatalf("handle: %v", err)
		}
	})

	t.Run("SNS -> SQS forward", func(t *testing.T) {
		msg := "sns-message"
		_, err := snsClient.Publish(&sns.PublishInput{TopicArn: &topicARN, Message: &msg})
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
		_ = json.Unmarshal([]byte(awsv1.StringValue(msgs[0].Body)), &env)
		if err := proc.HandleSNS(ctx, env.Message, fwdQueueURL); err != nil {
			t.Fatalf("forward: %v", err)
		}
		fwd := receiveOnce(t, sqsClient, fwdQueueURL)
		if len(fwd) == 0 || awsv1.StringValue(fwd[0].Body) != msg {
			t.Fatalf("forward mismatch: %#v", fwd)
		}
	})

	t.Run("S3 -> SQS -> DynamoDB", func(t *testing.T) {
		// Configure bucket notification to queue
		setBucketQueueNotification(t, s3Client, bucket, queueARNFromURL(queueURL))
		// Put object
		key := "test.txt"
		_, err := s3Client.PutObject(&s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: awsv1.ReadSeekCloser(stringsReader("content"))})
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
		_ = json.Unmarshal([]byte(awsv1.StringValue(msgs[0].Body)), &ev)
		if len(ev.Records) == 0 {
			t.Fatalf("no records")
		}
		if err := proc.HandleS3Object(ctx, ev.Records[0].S3.Bucket.Name, ev.Records[0].S3.Object.Key); err != nil {
			t.Fatalf("handle s3: %v", err)
		}
	})

	t.Run("DynamoDB Streams replicate", func(t *testing.T) {
		// Insert an item to source table to generate a stream record
		_, err := db.PutItem(&dynamodb.PutItemInput{TableName: &table, Item: map[string]*dynamodb.AttributeValue{
			"pk": {S: awsv1.String("pk-1")}, "sk": {S: awsv1.String(time.Now().UTC().Format(time.RFC3339Nano))}, "payload": {S: awsv1.String("p")},
		}})
		if err != nil {
			t.Fatalf("put: %v", err)
		}
		// Read from streams and replicate
		desc, err := streams.DescribeStream(&dynamodbstreams.DescribeStreamInput{StreamArn: getStreamARN(t, db, table)})
		if err != nil || desc.StreamDescription == nil || len(desc.StreamDescription.Shards) == 0 {
			t.Fatalf("describe: %v", err)
		}
		itOut, err := streams.GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
			StreamArn:         desc.StreamDescription.StreamArn,
			ShardId:           desc.StreamDescription.Shards[0].ShardId,
			ShardIteratorType: awsv1.String(dynamodbstreams.ShardIteratorTypeTrimHorizon),
		})
		if err != nil {
			t.Fatalf("iterator: %v", err)
		}
		it := itOut.ShardIterator
		proc2 := proc
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			rec, _ := streams.GetRecords(&dynamodbstreams.GetRecordsInput{ShardIterator: it})
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
		out, err := db.Scan(&dynamodb.ScanInput{TableName: &replicaTable, Select: awsv1.String("COUNT")})
		if err != nil || awsv1.Int64Value(out.Count) == 0 {
			t.Fatalf("replica count: %v %v", err, out)
		}
	})
}

func receiveOnce(t *testing.T, client *sqs.SQS, queueURL string) []*sqs.Message {
	out, err := client.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: awsv1.Int64(1),
		WaitTimeSeconds:     awsv1.Int64(5),
	})
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	return out.Messages
}

func createDynamoTable(t *testing.T, db *dynamodb.DynamoDB, name string, withStream bool) {
	attr := []*dynamodb.AttributeDefinition{{AttributeName: awsv1.String("pk"), AttributeType: awsv1.String("S")}, {AttributeName: awsv1.String("sk"), AttributeType: awsv1.String("S")}}
	key := []*dynamodb.KeySchemaElement{{AttributeName: awsv1.String("pk"), KeyType: awsv1.String("HASH")}, {AttributeName: awsv1.String("sk"), KeyType: awsv1.String("RANGE")}}
	in := &dynamodb.CreateTableInput{TableName: &name, AttributeDefinitions: attr, KeySchema: key, ProvisionedThroughput: &dynamodb.ProvisionedThroughput{ReadCapacityUnits: awsv1.Int64(5), WriteCapacityUnits: awsv1.Int64(5)}}
	if withStream {
		in.StreamSpecification = &dynamodb.StreamSpecification{StreamEnabled: awsv1.Bool(true), StreamViewType: awsv1.String("NEW_IMAGE")}
	}
	_, _ = db.CreateTable(in)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		out, _ := db.DescribeTable(&dynamodb.DescribeTableInput{TableName: &name})
		if out != nil && out.Table != nil && awsv1.StringValue(out.Table.TableStatus) == dynamodb.TableStatusActive {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("table not active: %s", name)
}

func getStreamARN(t *testing.T, db *dynamodb.DynamoDB, table string) *string {
	out, err := db.DescribeTable(&dynamodb.DescribeTableInput{TableName: &table})
	if err != nil || out.Table == nil || out.Table.LatestStreamArn == nil {
		t.Fatalf("stream arn: %v", err)
	}
	return out.Table.LatestStreamArn
}

func createBucket(t *testing.T, s3c *s3.S3, name string) {
	_, _ = s3c.CreateBucket(&s3.CreateBucketInput{Bucket: &name})
}

func createQueue(t *testing.T, sqsClient *sqs.SQS, name string) string {
	out, err := sqsClient.CreateQueue(&sqs.CreateQueueInput{QueueName: &name})
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	return awsv1.StringValue(out.QueueUrl)
}

func queueARNFromURL(url string) string {
	// LocalStack pattern: http://localhost:4566/000000000000/queueName
	parts := strings.Split(url, "/")
	name := parts[len(parts)-1]
	return fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:%s", name)
}

func setBucketQueueNotification(t *testing.T, s3c *s3.S3, bucket string, queueARN string) {
	_, err := s3c.PutBucketNotificationConfiguration(&s3.PutBucketNotificationConfigurationInput{
		Bucket: &bucket,
		NotificationConfiguration: &s3.NotificationConfiguration{
			QueueConfigurations: []*s3.QueueConfiguration{{
				QueueArn: awsv1.String(queueARN),
				Events:   []*string{awsv1.String("s3:ObjectCreated:*")},
			}},
		},
	})
	if err != nil {
		t.Fatalf("set notif: %v", err)
	}
}

func createTopic(t *testing.T, snsClient *sns.SNS, arn string) string {
	name := arn[strings.LastIndex(arn, ":")+1:]
	out, err := snsClient.CreateTopic(&sns.CreateTopicInput{Name: &name})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	return awsv1.StringValue(out.TopicArn)
}

func subscribeQueueToTopic(t *testing.T, sqsClient *sqs.SQS, snsClient *sns.SNS, queueURL string, topicARN string) {
	attrs, _ := sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{QueueUrl: &queueURL, AttributeNames: []*string{awsv1.String("QueueArn")}})
	queueARN := attrs.Attributes["QueueArn"]
	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"sqs:SendMessage","Resource":"%s","Condition":{"ArnEquals":{"aws:SourceArn":"%s"}}}]}`, *queueARN, topicARN)
	_, _ = sqsClient.SetQueueAttributes(&sqs.SetQueueAttributesInput{QueueUrl: &queueURL, Attributes: map[string]*string{"Policy": &policy}})
	_, err := snsClient.Subscribe(&sns.SubscribeInput{TopicArn: &topicARN, Protocol: awsv1.String("sqs"), Endpoint: queueARN})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
}

// helpers
func stringsReader(s string) *strings.Reader { return strings.NewReader(s) }

func modelsItemFromImage(img map[string]*dynamodbstreams.AttributeValue) models.Item {
	return models.Item{PK: awsv1.StringValue(img["pk"].S), SK: awsv1.StringValue(img["sk"].S), Payload: awsv1.StringValue(img["payload"].S)}
}
