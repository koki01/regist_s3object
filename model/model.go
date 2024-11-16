package model

// 性別
type SexFlag int

const (
	unknown SexFlag = iota
	man
	woman
)

// メッセージモデル
type Message struct {
	Team       string  `json:"team"`
	Name       string  `json:"name"`
	Age        int     `json:"age"`
	Sex        SexFlag `json:"sex"`
	ObjectName string  `json:"object_name"`
}
