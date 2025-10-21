package p2pnetwork

import (
	"fmt"

	"github.com/sakesake/PBFT/message"
)

type SimulationP2P struct {
	Send       func(msg interface{}, msgChan chan<- *message.ConMessage)
	MsgChan    chan<- *message.ConMessage
	TotalNodes int
}

func NewSimP2pLib(
	totalNodes int,
	sendFunc func(msg interface{}, msgChan chan<- *message.ConMessage),
	msgChan chan<- *message.ConMessage,
) P2pNetwork {
	return &SimulationP2P{
		TotalNodes: totalNodes,
		Send:       sendFunc,
		MsgChan:    msgChan,
	}
}

func (sp *SimulationP2P) BroadCast(v interface{}) error {
	for i := 0; i < sp.TotalNodes; i++ {

		// launch a goroutine for each send
		go func(to int) {
			sp.Send(v, sp.MsgChan)
		}(i)
	}

	return nil
}

func (sp *SimulationP2P) SendToNode(nodeID int64, v interface{}) error {
	for i := 0; i < sp.TotalNodes; i++ {
		if i == int(nodeID) {
			conMsg, ok := v.(*message.ConMessage)
			if !ok {
				return fmt.Errorf("SendToNode: expected *message.ConMessage, got %T", v)
			}
			sp.MsgChan <- conMsg
			return nil
		}
	}
	return fmt.Errorf("Send to node failed. Node ID: {%d}", nodeID)
}
