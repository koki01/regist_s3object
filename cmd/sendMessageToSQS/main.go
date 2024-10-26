package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"regist_s3object/model"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func handler(ctx context.Context, s3Event events.S3Event) error {

	sdkconfig, _ := config.LoadDefaultConfig(ctx)

	s3Client := s3.NewFromConfig(sdkconfig)

	var bucket, key string

	//UPLOADされたオブジェクトの情報を取得
	for _, record := range s3Event.Records {

		bucket = record.S3.Bucket.Name

		key = record.S3.Object.Key
	}

	//対象オブジェクトを読み込む
	object, _ := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	log.Printf("get detail bucket %s object %s", bucket, key)

	content := object.Body

	defer content.Close()

	binary, _ := io.ReadAll(content)

	value := string(binary)

	log.Printf("get value %s", value)

	arr := strings.Split(value, ",")

	var msg model.Message

	msg.Team = arr[0]
	msg.Name = arr[1]
	msg.Age, _ = strconv.Atoi(arr[2])

	//読み込んだ値をSQSへ
	queueURL := os.Getenv("QUEUE_URL")

	msgJson, _ := json.Marshal(msg)

	sqsClient := sqs.NewFromConfig(sdkconfig)

	_, err := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: aws.String(string(msgJson)),
		QueueUrl:    &queueURL,
	})

	if err != nil {
		fmt.Printf("error occur %s", err)
	}

	return nil
}

func main() {
	lambda.Start(handler)
}
