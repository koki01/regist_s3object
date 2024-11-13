package model

// メッセージモデル
type Message struct {
	Team       string `json:"team"`
	Name       string `json:"name"`
	Age        int    `json:"age"`
	ObjectName string `json:"object_name"`
}
