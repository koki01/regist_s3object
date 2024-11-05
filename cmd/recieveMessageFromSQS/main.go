package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"regist_s3object/model"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/go-sql-driver/mysql"
)

func handler(sqsEvent events.SQSEvent) {

	for _, record := range sqsEvent.Records {
		process(record)
	}

	fmt.Println("done")
}

func process(message events.SQSMessage) {

	fmt.Printf("message: %s¥n", message.Body)

	//受信したメッセージを構造体へ
	var value model.Message
	err := json.Unmarshal([]byte(message.Body), &value)

	if err != nil {
		fmt.Printf("read ng: %s¥n", err)

	} else {
		fmt.Printf("read ok: Team %s Name %s Age %d", value.Team, value.Name, value.Age)
	}

	//DBへ接続
	db, err := intitDB()

	if err != nil {
		fmt.Printf("db connection ng: %s", err)
		return
	}

	defer db.Close()

	//DBへ登録する
	err = insertRecord(db, value)

	if err != nil {
		fmt.Printf("insert ng: %s", err)
	} else {
		fmt.Print("insert ok")
	}
}

// DB接続
func intitDB() (*sql.DB, error) {

	dbname := os.Getenv("DATABASE")
	user := os.Getenv("USER")
	pass := os.Getenv("PASS")
	endpoint := os.Getenv("ENDPOINT")

	fmt.Printf("db connect start: database %s user %s pass %s", dbname, user, pass)

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:3306)/%s", user, pass, endpoint, dbname))

	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	fmt.Printf("db connect ok")
	return db, nil
}

// INSERT実行
func insertRecord(db *sql.DB, msg model.Message) error {

	stmt, err := db.Prepare("INSERT INTO player(team,name,age) VALUES(?,?,?)")

	if err != nil {
		return err
	}

	defer stmt.Close()

	_, err = stmt.Exec(msg.Team, msg.Name, msg.Age)

	if err != nil {
		return err
	} else {
		return nil
	}
}

func main() {
	lambda.Start(handler)
}
