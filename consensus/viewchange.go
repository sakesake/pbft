package consensus

import (
	"fmt"

	"github.com/sakesake/PBFT/message"
)

type VCCache struct {
	vcMsg message.VMessage
	nvMsg map[int64]*message.NewView
}

func NewVCCache() *VCCache {
	return &VCCache{
		vcMsg: make(message.VMessage),
		nvMsg: make(map[int64]*message.NewView),
	}
}

func (vcc *VCCache) pushVC(vc *message.ViewChange) {
	vcc.vcMsg[vc.NodeID] = vc
}

func (vcc *VCCache) hasNewViewYet(vid int64) bool {
	if _, ok := vcc.nvMsg[vid]; ok {
		return true
	}
	return false
}

func (vcc *VCCache) addNewView(nv *message.NewView) {
	vcc.nvMsg[nv.NewViewID] = nv
}

/*
	The view-change protocol provides Aliveness by allowing the system to make progress when the primary fails. View
changes are triggered by timeouts that prevent backups from waiting indefinitely for requests to execute. A backup is
waiting for a Request if it received a valid Request and has not executed it. A backup starts a timer when it receives a
Request and the timer is not already running. It stops the timer when it is no longer waiting to execute the Request,
but restarts it if at that point it is waiting to execute some other Request.

	If the timer of backup i expires in view , the backup starts a view change to move the system to view v + 1.
It stops accepting messages (other than checkpoint, view-change, and new-view messages) and multicasts a
<VIEW-CHANGE, v + 1, n, C, P, i> message to all replicas. Here  n is the sequence number of the last   stable
checkpoint s known to i, C is a set of 2f + 1 valid checkpoint messages proving the correctness of s and P is a set
containing  a set Pm for each Request m that prepared at i with a sequence number higher than n. Each set Pm contains a
valid pre-Prepare message(without the corresponding client message) and 2f matching, valid Prepare message signed by
different backups with same view, sequence number, and the digest of m.
*/

/*
Data Structures
	Replicas record information about what happened in earlier views. This information is maintained in two sets, P
and Q. These sets only contain information for sequence numbers between the current low and high water marks in the log.
The sets allow the view change protocol to work properly even when more than one view change occurs before the system is
able to continue normal operation; the sets are empty while the system is running normally. Replicas also store the
requests corresponding to entries in these sets.
	P at replica i stores information about requests that have prepared at i in previous views. Its entries are
tuples ⟨n, d , v⟩, meaning that i collected a prepared certificate for a request with digest d with number n in view v
and no request prepared at i in a later view with the same number.
	Q stores information about requests that have pre-prepared at i in previous views (i.e., requests for which i
has sent a PRE-PREPARE or PREPARE message). Its entries are tuples ⟨n, d , v⟩, meaning that i pre-prepared a request
with digest d with number n in view v and that request did not pre-prepare at i in a later view with the same number.
*/

func (s *StateEngine) computePMsg() map[int64]*message.PTuple {
	P := make(map[int64]*message.PTuple)
	for seq := s.MiniSeq; seq < s.MaxSeq; seq++ {
		log, ok := s.msgLogs[seq]
		if !ok || log.Stage < Prepared {
			continue
		}

		fmt.Printf("[computePMsg] Seq: %d, log.Prepare len: %d\n", seq, len(log.Prepare))

		tuple := &message.PTuple{
			PPMsg: log.PrePrepare,
			PMsg:  log.Prepare,
		}
		P[seq] = tuple
	}

	return P
}

/*
View-Change Messages

	When a backup i suspects the primary for view v is faulty, it enters view v + 1 and multicasts a

⟨VIEW-CHANGE, v + 1, h, C, P, Q, i⟩αi message to all replicas. Here h is the sequence number of the latest stable
checkpoint known to i, C is a set of pairs with the sequence number and digest of each checkpoint stored at i,
and P and Q are the sets described above. These sets are updated before sending the VIEW-CHANGE message using
the information in the log, as explained in Figure 3. Once the VIEW-CHANGE message has been sent, i removes PRE-PREPARE,
PREPARE, and COMMIT messages from its log. The number of tuples in Q may grow without bound if the algorithm changes
views repeatedly without making progress. In Castro [2001], we describe a modification to the algorithm that bounds
the size of the Q by a constant. It is interesting to note that VIEW-CHANGE messages do not include PRE-PREPARE, PREPARE,
or CHECKPOINT messages.
*/
func (s *StateEngine) ViewChange() {

	fmt.Printf("======>[ViewChange] (%d, %d).....\n", s.CurViewID, s.lastCP.Seq)
	s.nodeStatus = ViewChanging
	s.Timer.tack()

	pMsg := s.computePMsg()

	vc := &message.ViewChange{
		NewViewID: s.CurViewID + 1,
		LastCPSeq: s.lastCP.Seq,
		NodeID:    s.NodeID,
		CMsg:      s.lastCP.CPMsg,
		PMsg:      pMsg,
	}

	nextPrimaryID := vc.NewViewID % message.TotalNodeNO
	if s.NodeID == nextPrimaryID {
		s.sCache.pushVC(vc) //[vc.NodeID] = vc
	}

	consMsg := message.CreateConMsg(message.MTViewChange, vc)
	if err := s.p2pWire.BroadCast(consMsg); err != nil {
		fmt.Println(err)
		return
	}
	s.CurViewID++
	s.msgLogs = make(map[int64]*NormalLog)
}

/*
View-Change-Ack Messages
	Replicas collect VIEW-CHANGE messages for v+1 and send acknowledgments for them to v + 1’s primary, p. Replicas
only accept these VIEW-CHANGE messages if all the information in their P and Q components is for view numbers less than
or equal to v. The acknowledgments have the form ⟨VIEW-CHANGE-ACK,v+1,i, j,d⟩μip, where i is the identifier of the
sender, d is the digest of the VIEW-CHANGE message being acknowledged, and j is the replica that sent that VIEW-CHANGE
message. These acknowledgments allow the primary to prove authenticity of VIEW-CHANGE messages sent by faulty replicas.
*/

type Set map[interface{}]bool

func (s Set) put(key interface{}) {
	s[key] = true
}
func (s *StateEngine) checkViewChange(vc *message.ViewChange) error {
	if s.CurViewID > vc.NewViewID {
		return fmt.Errorf("it's[%d] not for me[%d] view change", vc.NewViewID, s.CurViewID)
	}
	if len(vc.CMsg) <= message.MaxFaultyNode {
		return fmt.Errorf("view message checking C message failed")
	}
	var counter = make(map[int64]Set)
	for id, cp := range vc.CMsg {
		if cp.ViewID >= vc.NewViewID {
			continue
		}

		if cp.SequenceID != vc.LastCPSeq {
			return fmt.Errorf("view change message C msg's n[]%d is different from vc's"+
				" h[%d]", cp.SequenceID, vc.LastCPSeq)
		}

		//TODO:: digest test
		//if cp.Digest != message.Digest(vc.LastCPSeq){
		//
		//}

		if counter[cp.ViewID] == nil {
			counter[cp.ViewID] = make(Set)
		}

		counter[cp.ViewID].put(id)
	}

	CMsgIsOK := false
	for vid, set := range counter {
		if len(set) > message.MaxFaultyNode {
			fmt.Printf("view change check C message success[%d]:\n", vid)
			CMsgIsOK = true
			break
		}
	}
	if !CMsgIsOK {
		return fmt.Errorf("no valid C message in view change msg")
	}

	counter = make(map[int64]Set)
	for seq, pt := range vc.PMsg {

		ppView := pt.PPMsg.ViewID
		//prePrimaryID :=  ppView % message.TotalNodeNO

		if seq <= vc.LastCPSeq || seq > vc.LastCPSeq+CheckPointK {
			return fmt.Errorf("view change message checking P message faild pre-prepare n=%d,"+
				" checkpoint h=%d", seq, vc.LastCPSeq)
		}
		if ppView >= vc.NewViewID {
			return fmt.Errorf("view change message checking P message faild pre-prepare view=%d,"+
				" new view id=%d", pt.PPMsg.ViewID, vc.NewViewID)
		}

		for nid, prepare := range pt.PMsg {
			if ppView != prepare.ViewID {
				return fmt.Errorf("view change message checking view id[%d] in pre-prepare is not "+
					"same as prepare's[%d]", ppView, prepare.ViewID)
			}
			if seq != prepare.SequenceID {
				return fmt.Errorf("view change message checking seq id[%d] in pre-prepare"+
					"is different from prepare's[%d]", seq, prepare.SequenceID)
			}
			counter[ppView].put(nid)
		}
	}

	PMsgIsOk := false
	for vid, set := range counter {
		if len(set) >= 2*message.MaxFaultyNode {
			fmt.Printf("view change check P message success[%d]:\n", vid)
			PMsgIsOk = true
			break
		}
	}
	if !PMsgIsOk {
		return fmt.Errorf("view change check p message failed")
	}

	return nil
}

func (s *StateEngine) procViewChange(vc *message.ViewChange) error {
	nextPrimaryID := vc.NewViewID % message.TotalNodeNO
	if s.NodeID != nextPrimaryID {
		fmt.Printf("im[%d] not the new[%d] primary node\n", s.NodeID, nextPrimaryID)
		return nil
	}
	if err := s.checkViewChange(vc); err != nil {
		return err
	}

	s.sCache.pushVC(vc)
	if len(s.sCache.vcMsg) < message.MaxFaultyNode*2 {
		return nil
	}
	if s.sCache.hasNewViewYet(vc.NewViewID) {
		fmt.Printf("view change[%d] is in processing......\n", vc.NewViewID)
		return nil
	}

	return s.createNewViewMsg(vc.NewViewID)
}

/*
New-View Message Construction
	The new primary p collects VIEW-CHANGE and VIEW-CHANGE-ACK messages (including messages from itself). It stores
VIEW-CHANGE messages in a set S. It adds a VIEW-CHANGE message received from replica i to S after receiving 2 f − 1
VIEW-CHANGE-ACKs for i’s VIEW-CHANGE message from other replicas. These VIEW-CHANGE-ACK messages together with the
VIEW-CHANGE message it received and the VIEW-CHANGE-ACK it could have sent form a quorum certificate. We call it the
view-change certificate. Each entry in S is for a different replica.
	The new primary uses the information in S and the decision procedure sketched in Figure 4 to choose a checkpoint
and a set of requests. This procedure runs each time the primary receives new information, for example, when it adds a
new message to S. We use the notation m.x to indicate component x of message m where x is the name we used for the
component when defining the format for m’s message type.
	The primary starts by selecting the checkpoint that is going to be the starting state for request processing in
the new view. It picks the checkpoint with the highest number h from the set of checkpoints that are known to be correct
(because they have a weak certificate) and that have numbers higher than the low water mark in the log of at least f + 1
non-faulty replicas. The last condition is necessary for live-ness; it ensures that the ordering information for requests
that committed with numbers higher than h is still available.
	Next, the primary selects a request to pre-prepare in the new view for each sequence number n between h and
h + L (where L is the size of the log). If a request m committed in a previous view, the primary must select m. If such
a request exists, it is guaranteed to be the only one that satisfies conditions A1 and A2. Condition A1 ensures that the
primary selects the request that some replica in a quorum claims to have prepared in the latest view v, and A2 ensures
that the request could prepare in view v because it was pre-prepared by at least one correct replica in v or a later view.
If there is a quorum of replicas that did not prepare any request with sequence number n (condition B), no request
committed with number n. There- fore, the primary selects a special null request that goes through the protocol as a
regular request but whose execution is a no-op. (Paxos [Lamport 1989] used a similar technique to fill in gaps.)
	The decision procedure ends when the primary has selected a request for each number. This may require waiting
for more than n− f messages but a primary is always able to complete the decision procedure once it receives all
VIEW-CHANGE messages sent by non-faulty replicas for its view. After deciding, the primary multicasts a NEW-VIEW message
to the other replicas with its decision: ⟨NEW-VIEW, v + 1, V , X ⟩α p . Here, V contains a pair for each entry in S
consisting of the identifier of the sending replica and the digest of its VIEW-CHANGE message, and X identifies the
checkpoint and request values selected. The VIEW-CHANGEs in V are the new-view certificate.
*/

func (s *StateEngine) GetON(newVID int64) (int64, int64, message.OMessage, message.OMessage, *message.ViewChange) {
	mergeP := make(map[int64]*message.PTuple)
	var maxNinV int64 = 0
	var maxNinO int64 = 0

	var cpVC *message.ViewChange = nil
	for _, vc := range s.sCache.vcMsg {
		if vc.LastCPSeq > maxNinV {
			maxNinV = vc.LastCPSeq
			cpVC = vc
		}
		for seq, pMsg := range vc.PMsg {
			if _, ok := mergeP[seq]; ok {
				continue
			}
			mergeP[seq] = pMsg
			if seq > maxNinO {
				maxNinO = seq
			}
		}
	}

	O := make(message.OMessage)
	N := make(message.OMessage)
	for i := maxNinV + 1; i <= maxNinO; i++ {
		pt, ok := mergeP[i]
		if ok {
			O[i] = pt.PPMsg
			O[i].ViewID = newVID
		} else {
			N[i] = &message.PrePrepare{
				ViewID:     newVID,
				SequenceID: i,
				Digest:     "",
			}
		}
	}

	return maxNinV, maxNinO, O, N, cpVC
}

func (s *StateEngine) createNewViewMsg(newVID int64) error {

	s.CurViewID = newVID
	newCP, newSeq, o, n, cpVC := s.GetON(newVID)
	nv := &message.NewView{
		NewViewID: s.CurViewID,
		VMsg:      s.sCache.vcMsg,
		OMsg:      o,
		NMsg:      n,
	}

	s.sCache.addNewView(nv)

	s.CurSequence = newSeq

	msg := message.CreateConMsg(message.MTNewView, nv)
	if err := s.p2pWire.BroadCast(msg); err != nil {
		return err
	}
	s.updateStateNV(newCP, cpVC)
	s.cleanRequest()
	return nil
}

/*
New-View Message Processing
	The primary updates its state to reflect the information in the NEW-VIEW mes- sage. It obtains any requests
in X that it is missing and if it does not have the checkpoint with sequence number h, it also initiates the protocol
to fetch the missing state (see Section 6.2.2). When it has all requests in X and the check- point with sequence number
h is stable, it records in its log that the requests are pre-prepared in view v + 1.
	The backups for view v + 1 collect messages until they have a correct NEW-VIEW message and a correct matching
VIEW-CHANGE message for each pair in V. If a backup did not receive one of the VIEW-CHANGE messages for some replica
with a pair in V, the primary alone may be unable to prove that the message it received is authentic because it is
not signed. The use of VIEW-CHANGE-ACK messages solves this problem. Since the primary only includes a VIEW-CHANGE
message in S after obtaining a matching view-change certificate, at least f + 1 non-faulty replicas can vouch for the
authenticity of every VIEW-CHANGE message whose di- gest is in V. Therefore, if the original sender of a VIEW-CHANGE is
uncooperative, the primary retransmits that sender’s VIEW-CHANGE message and the non-faulty backups retransmit their
VIEW-CHANGE-ACKs. A backup can accept a VIEW-CHANGE message whose authenticator is incorrect if it receives f
VIEW-CHANGE-ACKs that match the digest and identifier in V.
	After obtaining the NEW-VIEW message and the matching VIEW-CHANGE mes- sages, the backups check if these
messages support the decisions reported by the primary by carrying out the decision procedure in Figure 4. If they do
not, the replicas move immediately to view v + 2. Otherwise, they modify their state to account for the new information
in a way similar to the primary. The only difference is that they multicast a PREPARE message for v + 1 for each request
they mark as pre-prepared. Thereafter, normal case operation resumes.
*/

func (s *StateEngine) updateStateNV(maxNV int64, vc *message.ViewChange) {

	if maxNV > s.lastCP.Seq {
		cp := NewCheckPoint(maxNV, s.CurViewID)
		cp.CPMsg = vc.CMsg
		s.checks[maxNV] = cp
		s.runCheckPoint(maxNV)

		s.createCheckPoint(maxNV)
	}

	if maxNV > s.LasExeSeq {
		//TODO:: last reply last reply time
		s.LasExeSeq = maxNV
	}

	return
}

func (s *StateEngine) cleanRequest() {
	for cid, client := range s.cliRecord {
		for seq, req := range client.Request {
			if req.TimeStamp < client.LastReplyTime {
				delete(client.Request, seq)
				fmt.Printf("cleaning request[%d] when view changed for client[%s]\n", seq, cid)
			}
		}
	}
	return
}

func (s *StateEngine) didChangeView(nv *message.NewView) error {

	newVID := nv.NewViewID
	s.sCache.vcMsg = nv.VMsg
	newCP, newSeq, O, N, cpVC := s.GetON(newVID)
	if !O.EQ(nv.OMsg) {
		return fmt.Errorf("new view checking O message faliled")
	}
	if !N.EQ(nv.NMsg) {
		return fmt.Errorf("new view checking N message faliled")
	}

	for _, ppMsg := range O {
		if e := s.idle2PrePrepare(ppMsg); e != nil {
			return e
		}
	}

	for _, ppMsg := range N {
		if e := s.idle2PrePrepare(ppMsg); e != nil {
			return e
		}
	}

	s.sCache.addNewView(nv)
	s.CurSequence = newSeq
	s.updateStateNV(newCP, cpVC)
	s.cleanRequest()
	return nil
}
