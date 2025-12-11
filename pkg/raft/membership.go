// Package raft provides membership types and serialization for dynamic cluster configuration.
package raft

import (
	"github.com/salahayoub/anchor/api"
	"google.golang.org/protobuf/proto"
)

// MembershipState represents a node's voting status in the cluster.
type MembershipState int

const (
	// NonVoter is a node that receives log replication but does not participate
	// in elections or commit quorum calculations.
	NonVoter MembershipState = iota
	// Voter is a full cluster member that participates in elections and
	// commit quorum calculations.
	Voter
)

// String returns a human-readable representation of the MembershipState.
func (s MembershipState) String() string {
	switch s {
	case NonVoter:
		return "NonVoter"
	case Voter:
		return "Voter"
	default:
		return "Unknown"
	}
}

// ClusterMember represents a node in the cluster configuration.
type ClusterMember struct {
	// ID is the unique identifier for this node.
	ID string
	// Address is the network address for this node.
	Address string
	// State is the voting status of this node.
	State MembershipState
}

// ClusterConfig represents the full cluster membership configuration.
type ClusterConfig struct {
	// Members contains all nodes in the cluster.
	Members []ClusterMember
}

// Equal returns true if two ClusterConfigs have the same members in the same order.
func (c *ClusterConfig) Equal(other *ClusterConfig) bool {
	if c == nil && other == nil {
		return true
	}
	if c == nil || other == nil {
		return false
	}
	if len(c.Members) != len(other.Members) {
		return false
	}
	for i, m := range c.Members {
		if m.ID != other.Members[i].ID ||
			m.Address != other.Members[i].Address ||
			m.State != other.Members[i].State {
			return false
		}
	}
	return true
}

// SerializeConfig serializes a ClusterConfig to bytes using protobuf.
// The serialized format uses the api.ClusterConfiguration protobuf message.
func SerializeConfig(config *ClusterConfig) ([]byte, error) {
	if config == nil {
		return nil, nil
	}

	// Convert to protobuf message
	pbConfig := &api.ClusterConfiguration{
		Servers: make([]*api.ServerInfo, len(config.Members)),
	}

	for i, member := range config.Members {
		pbConfig.Servers[i] = &api.ServerInfo{
			Id:      member.ID,
			Address: member.Address,
			IsVoter: member.State == Voter,
		}
	}

	return proto.Marshal(pbConfig)
}

// DeserializeConfig deserializes bytes to a ClusterConfig using protobuf.
// The serialized format uses the api.ClusterConfiguration protobuf message.
func DeserializeConfig(data []byte) (*ClusterConfig, error) {
	if len(data) == 0 {
		return &ClusterConfig{}, nil
	}

	pbConfig := &api.ClusterConfiguration{}
	if err := proto.Unmarshal(data, pbConfig); err != nil {
		return nil, err
	}

	config := &ClusterConfig{
		Members: make([]ClusterMember, len(pbConfig.Servers)),
	}

	for i, server := range pbConfig.Servers {
		state := NonVoter
		if server.IsVoter {
			state = Voter
		}
		config.Members[i] = ClusterMember{
			ID:      server.Id,
			Address: server.Address,
			State:   state,
		}
	}

	return config, nil
}
