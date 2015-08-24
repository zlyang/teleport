//socket长连接， JSON 数据传输包。
package teleport

import (
  "github.com/golang/protobuf/proto"
)

const (
  // 返回成功
  SUCCESS = 0
  // 返回失败
  FAILURE = -1
  // 返回非法请求
  LLLEGAL = -2
)

// 定义数据传输结构
type NetData struct {
  // 消息体
  Body interface{}
  // 操作代号
  Operation string
  // 唯一标识符
  UID string
  // 发信节点uid
  From string
  // 收信节点uid
  To string
  // 返回状态
  Status int
}

func NewNetData(from, to, operation string, body interface{}) *NetData {
  return &NetData{
    From:      from,
    To:        to,
    Body:      body,
    Operation: operation,
    Status:    SUCCESS,
  }
}

func ProtoNetData2(data []byte, conn *Connect) (*NetData, error) {
  d := new(NetData)
  protoNetData := new(NetDataProto)
  err := proto.Unmarshal(data, protoNetData)
  if err == nil {
    if d.From == "" {
      d.From = conn.UID
    }

    d.Body = protoNetData.GetBody()
    d.Operation = protoNetData.GetOperation()
    d.UID = protoNetData.GetUID()
    d.From = protoNetData.GetFrom()
    d.To = protoNetData.GetTo()
    d.Status = int(protoNetData.GetStatus())
  }

  return d, err
}
