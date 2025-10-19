package p2pnetwork

type SimulationP2P struct {
	Send       func(msg interface{})
	TotalNodes int
}

func NewSimP2pLib(totalNodes int, sendFunc func(msg interface{})) P2pNetwork {
	return &SimulationP2P{
		TotalNodes: totalNodes,
		Send:       sendFunc,
	}
}

func (sp *SimulationP2P) BroadCast(v interface{}) error {
	for i := 0; i < sp.TotalNodes; i++ {

		// launch a goroutine for each send
		go func(to int) {
			sp.Send(v)
		}(i)
	}

	return nil
}

func (sp *SimulationP2P) SendToNode(nodeID int64, v interface{}) error {
	//TODO:: single point message
	return sp.BroadCast(v)
}
