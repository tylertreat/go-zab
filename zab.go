package zab

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/gdamore/mangos"
	"github.com/gdamore/mangos/protocol/pull"
	"github.com/gdamore/mangos/protocol/push"
	"github.com/gdamore/mangos/protocol/respondent"
	"github.com/gdamore/mangos/protocol/surveyor"
	"github.com/gdamore/mangos/transport/tcp"
)

const (
	leaderTimeout     = 5 * time.Second
	followerTimeout   = 5 * time.Second
	heartbeatTimeout  = time.Second
	heartbeatInterval = 50 * time.Millisecond
)

type state uint8

const (
	following state = iota
	leading
)

// zxid is a transaction identifier.
type zxid struct {
	Epoch   uint64 `json:"epoch"`
	Counter uint64 `json:"counter"`
}

type value struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// transaction is the state change a primary propagates to the backup
// Processes. State changes are idempotent.
type transaction struct {
	Zxid  *zxid  `json:"zxid"`
	Value *value `json:"value"`
}

// ServiceUpCall is called when a transaction is delivered.
type ServiceUpCall func(key, value []byte)

// Process is a participant in the Zab protocol. A Process is either a
// follower, leader, or proposed leader. There is only one established leader
// at a given time. A leader is also a follower and is not established until
// phase 2 completes. The election state is defined implicitly.
type Process struct {
	id               string
	broadcastAddr    string
	pull             mangos.Socket
	epoch            uint64
	peers            map[string]*Peer
	oracle           Oracle
	state            state
	leader           *Peer
	zxid             *zxid
	leaderHeartbeat  mangos.Socket
	heartbeatTimeout chan bool
	heartbeatAddr    string
	proposals        chan *transaction
	deliver          ServiceUpCall
}

type Peer struct {
	BroadcastAddr string
	HeartbeatAddr string
	push          mangos.Socket
}

type Peers []*Peer

func (p Peers) Len() int           { return len(p) }
func (p Peers) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p Peers) Less(i, j int) bool { return p[i].BroadcastAddr < p[j].BroadcastAddr }

type messageType uint8

const (
	cepoch messageType = iota
	newEpoch
	ackEpoch
	newLeader
	ackLeader
	commit
	heartbeat
	propose
	ackPropose
)

type message struct {
	Addr  string       `json:"addr"`
	Type  messageType  `json:"type"`
	Epoch uint64       `json:"epoch"`
	Zxid  *zxid        `json:"zxid"`
	Txn   *transaction `json:"txn"`
}

// NewProcess creates a new Zab Process. It returns an error if the Process
// cannot be created.
func NewProcess(id string, local *Peer, peers Peers, upCall ServiceUpCall) (*Process, error) {
	pull, err := pull.NewSocket()
	if err != nil {
		return nil, err
	}
	pull.AddTransport(tcp.NewTransport())
	leaderHeartbeat, err := surveyor.NewSocket()
	if err != nil {
		return nil, err
	}
	leaderHeartbeat.AddTransport(tcp.NewTransport())
	leaderHeartbeat.SetOption(mangos.OptionSurveyTime, heartbeatTimeout)
	peerMap := make(map[string]*Peer, len(peers))
	for _, peer := range peers {
		push, err := push.NewSocket()
		if err != nil {
			return nil, err
		}
		push.AddTransport(tcp.NewTransport())
		peer.push = push
		peerMap[peer.BroadcastAddr] = peer
	}
	return &Process{
		id:               id,
		broadcastAddr:    local.BroadcastAddr,
		pull:             pull,
		oracle:           NewRoundRobinOracle(append(peers, local)),
		peers:            peerMap,
		leaderHeartbeat:  leaderHeartbeat,
		heartbeatTimeout: make(chan bool),
		heartbeatAddr:    local.HeartbeatAddr,
		proposals:        make(chan *transaction, 100),
		deliver:          upCall,
		zxid:             &zxid{Epoch: 0, Counter: 0},
	}, nil
}

// Start begins execution of the Zab protocol for this Process. This will block
// for the duration of execution or return an error if a non-recoverable error
// occurs during execution.
func (p *Process) Start() error {
	if err := p.pull.Listen(p.broadcastAddr); err != nil {
		return err
	}
	if err := p.leaderHeartbeat.Listen(p.heartbeatAddr); err != nil {
		return err
	}
	for addr, peer := range p.peers {
		if err := peer.push.Dial(addr); err != nil {
			return err
		}
	}

	for {
		leader, err := p.oracle.Leader()
		if err != nil {
			return err
		}
		p.leader = leader
		if p.leader.BroadcastAddr == p.broadcastAddr {
			// This Process is the proposed leader.
			p.state = leading
			if err := p.leaderLoop(); err != nil {
				log.Println("Leader error:", err.Error())
			}
		} else {
			p.state = following
			if err := p.followerLoop(); err != nil {
				log.Println("Follower error:", err.Error())
			}
		}
	}
}

// Propose will propose a transaction to the system. It returns an error if the
// Process is not the current leader or too many transactions are currently in
// progress. It returns nil if the transaction was successfully proposed (this
// does not guarantee it has been delivered).
func (p *Process) Propose(key, val []byte) error {
	if p.state != leading {
		return errors.New("Process not established leader")
	}

	select {
	case p.proposals <- p.newTransaction(&value{Key: key, Value: val}):
		return nil
	default:
		return errors.New("Too many in-flight proposals")
	}
}

func (p *Process) leaderLoop() error {
	if err := p.leaderDiscovery(); err != nil {
		return err
	}

	if err := p.leaderSynchronization(); err != nil {
		return err
	}

	return p.leaderBroadcast()
}

func (p *Process) leaderDiscovery() error {
	log.Println("Starting leader discovery")
	if err := p.leaderNewEpoch(); err != nil {
		return err
	}

	return p.leaderAckEpoch()
}

func (p *Process) leaderSynchronization() error {
	log.Println("Starting leader synchronization")

	// The proposed leader sends a new leader (NEWLEADER) message to each
	// member of the quorum containing the epoch number of the newly launched
	// epoch, together with the initial history for that epoch.
	if err := p.broadcast(p.newNewLeader()); err != nil {
		return err
	}

	// Once the proposed leader has received a quorum of acknowledgements it is
	// established as the new leader, and sends a commit message to all
	// followers.
	quorum := make(map[string]bool, p.quorum())
	timeout := time.Now().Add(leaderTimeout)
	for time.Now().Before(timeout) {
		msg, err := p.recv(leaderTimeout)
		if err != nil {
			continue
		}
		if msg.Type != ackLeader {
			continue
		}

		quorum[msg.Addr] = true
		if len(quorum) < p.quorum() {
			continue
		}

		return p.broadcast(p.newCommit(nil))
	}

	return errors.New("Leader timeout")
}

func (p *Process) leaderBroadcast() error {
	log.Println("Starting leader broadcast")

	// To mutually detect crashes in a fine-grained and convenient manner,
	// avoiding operating system reconfiguration, leader and followers exchange
	// periodic heartbeats. If the leader does not receive heartbeats from a
	// quorum of followers within a timeout interval, the leader renounces
	// leadership of the epoch, and transitions to the ELECTION state.
	go p.heartbeatLoop()

	for {
		select {
		case txn := <-p.proposals:
			// TODO: Allow multiple outstanding proposals.
			if err := p.propose(txn); err != nil {
				log.Println("Proposal failed:", err.Error())
				continue
			}
		case <-p.heartbeatTimeout:
			return errors.New("Leader relinquished")
		}
	}

	return nil
}

func (p *Process) propose(txn *transaction) error {
	if err := p.broadcast(p.newPropose(txn)); err != nil {
		return err
	}
	acks := make(map[string]bool, p.quorum())
	timeout := time.Now().Add(leaderTimeout)
	for time.Now().Before(timeout) {
		msg, err := p.recv(leaderTimeout)
		if err != nil {
			continue
		}
		if msg.Type != ackPropose {
			continue
		}

		acks[msg.Addr] = true
		if len(acks) < p.quorum() {
			continue
		}

		if err := p.broadcast(p.newCommit(txn)); err != nil {
			return err
		}

		// Deliver transaction.
		p.deliver(txn.Value.Key, txn.Value.Value)
		return nil
	}

	return errors.New("timeout")
}

func (p *Process) leaderNewEpoch() error {
	// Once the prospective leader has received CEPOCH messages from a quorum
	// of followers it assigns a new epoch number which is greater than any
	// sent by the followers in those messages, and sends a NEWEPOCH message to
	// each member of the quorum including this new epoch number.
	quorum := make(map[string]uint64, p.quorum())
	timeout := time.Now().Add(leaderTimeout)
	for time.Now().Before(timeout) {
		msg, err := p.recv(leaderTimeout)
		if err != nil {
			continue
		}
		if msg.Type != cepoch {
			continue
		}

		quorum[msg.Addr] = msg.Epoch
		if len(quorum) < p.quorum() {
			continue
		}

		epoch := p.nextEpoch(quorum)
		if err := p.broadcast(p.newNewEpoch(epoch)); err != nil {
			return err
		}

		p.epoch = epoch
		return nil
	}

	return errors.New("Leader timeout")
}

func (p *Process) leaderAckEpoch() error {
	// When the proposed leader has received an epoch acknowledgement from each
	// member of the quorum, it chooses the most up to date history from
	// amongst all those sent in the acknowledgements to become the initial
	// history of the new epoch. The most up to date history is the one with
	// the highest epoch number, and within that epoch, the highest transaction
	// id.
	acks := make(map[string]*zxid, p.quorum())
	timeout := time.Now().Add(leaderTimeout)
	for time.Now().Before(timeout) {
		msg, err := p.recv(leaderTimeout)
		if err != nil {
			continue
		}
		if msg.Type != ackEpoch {
			continue
		}

		acks[msg.Addr] = msg.Zxid
		if len(acks) < p.quorum() {
			continue
		}

		// TODO: copy history.
		fmt.Println("leader copy history")
		return nil
	}

	return errors.New("Leader timeout")
}

func (p *Process) followerLoop() error {
	if err := p.followerDiscovery(); err != nil {
		return err
	}

	if err := p.followerSynchronization(); err != nil {
		return err
	}

	return p.followerBroadcast()
}

func (p *Process) followerDiscovery() error {
	log.Println("Starting follower discovery")

	// Followers send a current epoch message (CEPOCH) to the prospective
	// leader (as chosen by the oracle), which includes the epoch number of the
	// last new epoch (NEWEPOCH) messasge they acknowledged if any.
	if err := p.sendToLeader(p.newCurrentEpoch()); err != nil {
		return err
	}

	// When a follower receives this NEWEPOCH message, it acknowledges it if
	// the epoch number is greater than that of the last epoch it previously
	// acknowledged. The epoch acknowledgement message sent by the following
	// includes the current epoch of the follower, as well as the follower’s
	// transaction history.
	timeout := time.Now().Add(followerTimeout)
	for time.Now().Before(timeout) {
		msg, err := p.recv(followerTimeout)
		if err != nil {
			continue
		}
		if msg.Type != newEpoch || msg.Addr != p.leader.BroadcastAddr {
			continue
		}

		if msg.Epoch <= p.epoch {
			log.Println("Received new epoch message with old epoch", msg.Epoch)
			continue
		}

		if err := p.sendToLeader(p.newAckEpoch(msg.Epoch)); err != nil {
			return err
		}

		p.epoch = msg.Epoch
		return nil
	}

	return errors.New("Follower timeout")
}

func (p *Process) followerSynchronization() error {
	log.Println("Starting follower synchronization")

	// When a follower receives a NEWLEADER message, it checks the epoch number
	// against the epoch number it last acknowledged. If these are different,
	// it starts a new iteration of the protocol. In the expected case of
	// course they will be the same, and the follower proceeds by accepting all
	// transactions in the initial history and setting its own history to
	// match. It then acknowledges the new leader proposal.
	timeout := time.Now().Add(followerTimeout)
	for time.Now().Before(timeout) {
		msg, err := p.recv(followerTimeout)
		if err != nil {
			continue
		}
		if msg.Type != newLeader || msg.Addr != p.leader.BroadcastAddr {
			continue
		}

		if msg.Epoch != p.epoch {
			return fmt.Errorf("Received new leader message with differing epoch", msg.Epoch)
		}

		// TODO: copy history.
		fmt.Println("follower copy history")

		if err := p.sendToLeader(p.newAckLeader()); err != nil {
			return err
		}

		// When a follower receives this commit message, it delivers (cf. VR’s
		// service up-call) each transaction in the initial history in order.
		timeout = time.Now().Add(followerTimeout)
		for time.Now().Before(timeout) {
			msg, err := p.recv(followerTimeout)
			if err != nil {
				continue
			}
			if msg.Type != commit || msg.Addr != p.leader.BroadcastAddr {
				continue
			}

			// TODO: deliver transactions.
			fmt.Println("follower deliver txns")
			return nil
		}
		break
	}

	return errors.New("Follower timeout")
}

func (p *Process) followerBroadcast() error {
	log.Println("Starting follower broadcast")

	// A follower only follows one leader at a time and stays connected to a
	// leader as long as it receives heartbeats within a timeout interval.
	go p.heartbeatLoop()

	// Followers accept transactions in the order that they receive them
	// (transaction id order), and deliver those transactions once they receive
	// the commit message and have delivered all previously accepted
	// transactions with lower ids.
	// TODO: Refactor to support multiple in-flight transactions.
	for {
		select {
		case <-p.heartbeatTimeout:
			return errors.New("Leader timed out")
		default:
		}

		msg, err := p.recv(followerTimeout)
		if err != nil {
			continue
		}
		if msg.Type != propose || msg.Addr != p.leader.BroadcastAddr {
			continue
		}

		if err := p.sendToLeader(p.newAckPropose()); err != nil {
			log.Println("Failed to ack transaction:", err.Error())
			continue
		}

		var (
			txn       = msg.Txn
			didCommit = false
			timeout   = time.Now().Add(followerTimeout)
		)
		for time.Now().Before(timeout) {
			msg, err = p.recv(followerTimeout)
			if err != nil {
				continue
			}
			if msg.Type != commit || msg.Addr != p.leader.BroadcastAddr {
				continue
			}
			if msg.Txn.Zxid.Counter != txn.Zxid.Counter {
				continue
			}

			p.deliver(txn.Value.Key, txn.Value.Value)
			didCommit = true
			break
		}
		if !didCommit {
			log.Println("Follower didn't receive commit")
		}
	}

	return nil
}

func (p *Process) heartbeatLoop() {
	var err error
	if p.state == leading {
		err = p.leaderHeartbeatLoop()
	} else {
		err = p.followerHeartbeatLoop()
	}
	log.Println("Heartbeat error:", err.Error())
	p.heartbeatTimeout <- true
}

func (p *Process) leaderHeartbeatLoop() error {
	for {
		if err := p.sendHeartbeat(p.leaderHeartbeat); err != nil {
			return err
		}

		heartbeats := 0
		for {
			msg, err := p.leaderHeartbeat.Recv()
			if err != nil {
				break
			}
			var message message
			if err := json.Unmarshal(msg, &message); err != nil {
				return err
			}
			if message.Type != heartbeat {
				continue
			}
			heartbeats++
			if heartbeats >= p.quorum() {
				break
			}
		}

		// If the leader does not receive heartbeats from a quorum of followers
		// within a timeout interval, the leader renounces leadership of the
		// epoch, and transitions to the ELECTION state.
		if heartbeats < p.quorum() {
			return errors.New("Leader did not receive quorum heartbeat")
		}

		time.Sleep(heartbeatInterval)
	}
}

func (p *Process) followerHeartbeatLoop() error {
	// A follower only follows one leader at a time and stays connected to a
	// leader as long as it receives heartbeats within a timeout interval.
	followerHeartbeat, err := respondent.NewSocket()
	if err != nil {
		return err
	}
	followerHeartbeat.AddTransport(tcp.NewTransport())
	if err := followerHeartbeat.Dial(p.leader.HeartbeatAddr); err != nil {
		return err
	}
	for {
		followerHeartbeat.SetOption(mangos.OptionRecvDeadline, heartbeatTimeout)
		msg, err := followerHeartbeat.Recv()
		if err != nil {
			return err
		}
		var message message
		if err := json.Unmarshal(msg, &message); err != nil {
			return err
		}
		if message.Type != heartbeat || message.Addr != p.leader.BroadcastAddr {
			continue
		}
		if err := p.sendHeartbeat(followerHeartbeat); err != nil {
			return err
		}
	}
}

func (p *Process) quorum() int {
	quorum := p.oracle.Quorum()
	if p.state == leading {
		quorum--
	}
	return quorum
}

func (p *Process) sendToLeader(msg *message) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	peer, ok := p.peers[p.leader.BroadcastAddr]
	if !ok {
		return fmt.Errorf("No leader %s", p.leader.BroadcastAddr)
	}
	return peer.push.Send(payload)
}

func (p *Process) broadcast(msg *message) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	quorum := p.quorum()
	sent := 0
	for _, peer := range p.peers {
		if e := peer.push.Send(payload); e != nil {
			err = e
		} else {
			sent++
		}
	}
	if sent >= quorum {
		err = nil
	}
	return err
}

func (p *Process) recv(timeout time.Duration) (*message, error) {
	p.pull.SetOption(mangos.OptionRecvDeadline, timeout)
	msg, err := p.pull.Recv()
	if err != nil {
		return nil, err
	}
	var message message
	err = json.Unmarshal(msg, &message)
	return &message, err
}

func (p *Process) newCurrentEpoch() *message {
	return &message{
		Addr:  p.broadcastAddr,
		Type:  cepoch,
		Epoch: p.epoch,
	}
}

func (p *Process) newNewEpoch(epoch uint64) *message {
	return &message{
		Addr:  p.broadcastAddr,
		Type:  newEpoch,
		Epoch: epoch,
	}
}

func (p *Process) newAckEpoch(epoch uint64) *message {
	return &message{
		Addr:  p.broadcastAddr,
		Type:  ackEpoch,
		Epoch: epoch,
		Zxid:  p.zxid,
	}
}

func (p *Process) newNewLeader() *message {
	return &message{
		Addr:  p.broadcastAddr,
		Type:  newLeader,
		Epoch: p.epoch,
		Zxid:  p.zxid,
	}
}

func (p *Process) newAckLeader() *message {
	return &message{
		Addr: p.broadcastAddr,
		Type: ackLeader,
	}
}

func (p *Process) newCommit(txn *transaction) *message {
	return &message{
		Addr: p.broadcastAddr,
		Type: commit,
		Txn:  txn,
	}
}

func (p *Process) newAckPropose() *message {
	return &message{
		Addr: p.broadcastAddr,
		Type: ackPropose,
	}
}

func (p *Process) newHeartbeat() *message {
	return &message{
		Addr: p.broadcastAddr,
		Type: heartbeat,
	}
}

func (p *Process) newPropose(txn *transaction) *message {
	return &message{
		Addr: p.broadcastAddr,
		Type: propose,
		Txn:  txn,
	}
}

func (p *Process) sendHeartbeat(socket mangos.Socket) error {
	payload, err := json.Marshal(p.newHeartbeat())
	if err != nil {
		return err
	}
	return socket.Send(payload)
}

func (p *Process) nextEpoch(quorum map[string]uint64) uint64 {
	quorum[p.broadcastAddr] = p.epoch
	highestEpoch := uint64(0)
	for _, epoch := range quorum {
		if epoch > highestEpoch {
			highestEpoch = epoch
		}
	}
	return highestEpoch + 1
}

func (p *Process) newTransaction(value *value) *transaction {
	zxid := &zxid{Epoch: p.zxid.Epoch, Counter: p.zxid.Counter + 1}
	return &transaction{Zxid: zxid, Value: value}
}
