package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	sdkconfig, err := config.LoadDefaultConfig(ctx)

	if err != nil {
		fmt.Printf("failed to get config: %s\n", err)
		return err
	}

	s3Client := s3.NewFromConfig(sdkconfig)

	var bucket, key string

	//UPLOADされたオブジェクトの情報を取得
	for _, record := range s3Event.Records {

		bucket = record.S3.Bucket.Name

		key = record.S3.Object.Key
	}

	//対象オブジェクトを取得
	object, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		fmt.Printf("failed to get object detail: %s\n", err)
		return err
	} else {
		fmt.Printf("get detail bucket %s object %s\n", bucket, key)
	}

	//取得したオブジェクトを読み込む
	content := object.Body

	defer content.Close()

	binary, err := io.ReadAll(content)

	if err != nil {
		fmt.Printf("failed to read data: %s\n", err)
		return err
	}

	value := string(binary)

	fmt.Printf("get value %s\n", value)

	arr := strings.Split(value, ",")

	if len(arr) != 3 {
		fmt.Print("file foramt error\n")
		return errors.New("file foramt error")
	}

	//取得した値を構造体へ
	var msg model.Message

	msg.Team = arr[0]
	msg.Name = arr[1]
	msg.Age, _ = strconv.Atoi(arr[2])

	//SQSへ送信
	queueURL := os.Getenv("QUEUE_URL")

	msgJson, _ := json.Marshal(msg)

	sqsClient := sqs.NewFromConfig(sdkconfig)

	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: aws.String(string(msgJson)),
		QueueUrl:    &queueURL,
	})

	if err != nil {
		fmt.Printf("failed to send message: %s\n", err)
		return err
	} else {
		fmt.Printf("successed to send message. Team %s Name %s Age %d\n", msg.Team, msg.Name, msg.Age)
	}

	return nil
}

func main() {
	lambda.Start(handler)
}
