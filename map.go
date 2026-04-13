package node

import (
	"context"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

func (v Version) less(other Version) bool {
	if v.Counter == other.Counter {
		return v.NodeID < other.NodeID
	}
	return v.Counter < other.Counter
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode

	allNodes []string
	mu       sync.RWMutex
	state    MapState
	clock    uint64
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	node := &CRDTMapNode{
		BaseNode: hive.NewBaseNode(id),
		allNodes: allNodeIDs,
		state:    MapState{},
		clock:    0,
	}

	return node
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	n.BaseNode.Start(ctx)
	go n.flood()

	return nil
}

func (n *CRDTMapNode) flood() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		snapshot := n.State()
		for _, peer := range n.allNodes {
			if peer == n.ID() {
				continue
			}
			_ = n.BaseNode.Send(peer, snapshot)
		}
	}
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state[k] = StateEntry{
		Value:     v,
		Tombstone: false,
		Version:   n.nextVersion(),
	}
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	entry, ok := n.state[k]
	if !ok || entry.Tombstone {
		return "", false
	}
	return entry.Value, true
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state[k] = StateEntry{
		Tombstone: true,
		Version:   n.nextVersion(),
	}
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for k, remoteEntry := range remote {
		local, exists := n.state[k]
		if !exists || local.Version.less(remoteEntry.Version) {
			n.state[k] = remoteEntry
		}
	}
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	n.mu.RLock()
	defer n.mu.RUnlock()

	out := make(MapState, len(n.state))
	for k, v := range n.state {
		out[k] = v
	}
	return out
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	out := make(map[string]string)
	for k, e := range n.state {
		if !e.Tombstone {
			out[k] = e.Value
		}
	}
	return out
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	if msg == nil || msg.Payload == nil {
		return nil
	}

	remote := msg.Payload.(MapState)
	n.Merge(remote)
	return nil
}

func (n *CRDTMapNode) nextVersion() Version {
	n.clock++
	return Version{n.clock + 1, n.ID()}
}
