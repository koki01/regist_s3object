package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"regist_s3object/model"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func handler(sqsEvent events.SQSEvent) {

	for _, record := range sqsEvent.Records {
		process(record)
	}

	fmt.Println("done")
}

func process(message events.SQSMessage) {

	//受信したメッセージを構造体へ
	var value model.Message

	fmt.Printf("message: %s¥n", message.Body)

	err := json.Unmarshal([]byte(message.Body), &value)

	if err != nil {
		fmt.Printf("read error: %s¥n", err)

	} else {
		fmt.Printf("Team: %s Name: %s Age: %d", value.Team, value.Name, value.Age)
	}

	//DBへ登録する
	db, err := intitDB()

	if err != nil {

		db.Close()
		fmt.Printf("db connection ng : %s", err)
		return
	}

	defer db.Close()

	err = insertRecord(db, value)

	if err != nil {
		fmt.Print(err)
	}

}

// DB接続
func intitDB() (*sql.DB, error) {

	dbname := os.Getenv("DATABASE")
	user := os.Getenv("USER")
	pass := os.Getenv("PASS")

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(db:3306)/%s", user, pass, dbname))

	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

// INSERT実行
func insertRecord(db *sql.DB, msg model.Message) error {

	stmt, err := db.Prepare("INSERT INTO sample_table VALUES(?,?,?)")

	if err != nil {
		stmt.Close()
		return fmt.Errorf("db prepare ng: %d", err)
	}

	defer stmt.Close()

	_, err = stmt.Exec(msg.Team, msg.Name, msg.Age)

	if err != nil {
		return fmt.Errorf("insert ng: %d", err)
	}

	return nil
}

func main() {

	lambda.Start(handler)
}
