package zab

import (
	"errors"
	"sort"
	"sync"
)

// Oracle is the leader oracle responsible for determining the prospective
// leader for the iteration.
type Oracle interface {
	// Leader returns the address of the prospective leader to follow.
	Leader() (*Peer, error)

	// Add the given address to the pool of processes.
	Add(addr *Peer)

	// Remove the given address from the pool of processes.
	Remove(addr *Peer)

	// Quorum returns the minimum number of nodes needed for a quorum.
	Quorum() int
}

// roundRobinOracle is an implementation of the Oracle interface which
// determines leaders in a round-robin fashion by rotating through IP addresses
// in ascending order.
type roundRobinOracle struct {
	peers        Peers
	currentIndex int
	mu           sync.Mutex
}

// NewRoundRobinOracle creates a new RoundRobinOracle with the given IP
// addresses.
func NewRoundRobinOracle(peers Peers) *roundRobinOracle {
	sort.Sort(peers)
	return &roundRobinOracle{peers: peers}
}

// Leader returns the address of the prospective leader to follow.
func (r *roundRobinOracle) Leader() (*Peer, error) {
	r.mu.Lock()
	if len(r.peers) == 0 {
		return nil, errors.New("No candidates")
	}
	if r.currentIndex >= len(r.peers) {
		r.currentIndex = 0
	}
	leader := r.peers[r.currentIndex]
	r.currentIndex++
	r.mu.Unlock()
	return leader, nil
}

// Add the given address to the pool of processes.
func (r *roundRobinOracle) Add(peer *Peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.peers {
		if existing.BroadcastAddr == peer.BroadcastAddr {
			// Already in the pool.
			return
		}
	}

	r.peers = append(r.peers, peer)
	sort.Sort(r.peers)
}

// Remove the given address from the pool of processes.
func (r *roundRobinOracle) Remove(peer *Peer) {
	r.mu.Lock()
	for i, existing := range r.peers {
		if existing.BroadcastAddr == peer.BroadcastAddr {
			r.peers = append(r.peers[:i], r.peers[i+1:]...)
			break
		}
	}
	r.mu.Unlock()
}

// Quorum returns the minimum number of nodes needed for a quorum.
func (r *roundRobinOracle) Quorum() int {
	r.mu.Lock()
	quorum := len(r.peers)/2 + 1
	r.mu.Unlock()
	return quorum
}
