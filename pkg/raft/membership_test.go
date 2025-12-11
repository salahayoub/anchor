// Package raft provides unit tests for membership types and serialization.
package raft

import (
	"testing"
)

// TestMembershipState_String tests the String method of MembershipState.
func TestMembershipState_String(t *testing.T) {
	tests := []struct {
		name     string
		state    MembershipState
		expected string
	}{
		{
			name:     "NonVoter",
			state:    NonVoter,
			expected: "NonVoter",
		},
		{
			name:     "Voter",
			state:    Voter,
			expected: "Voter",
		},
		{
			name:     "Unknown state",
			state:    MembershipState(99),
			expected: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.state.String()
			if result != tt.expected {
				t.Errorf("MembershipState(%d).String() = %q, want %q",
					tt.state, result, tt.expected)
			}
		})
	}
}

// TestClusterConfig_Equal tests the Equal method of ClusterConfig.
func TestClusterConfig_Equal(t *testing.T) {
	tests := []struct {
		name     string
		config1  *ClusterConfig
		config2  *ClusterConfig
		expected bool
	}{
		{
			name:     "both nil",
			config1:  nil,
			config2:  nil,
			expected: true,
		},
		{
			name:    "first nil",
			config1: nil,
			config2: &ClusterConfig{
				Members: []ClusterMember{},
			},
			expected: false,
		},
		{
			name: "second nil",
			config1: &ClusterConfig{
				Members: []ClusterMember{},
			},
			config2:  nil,
			expected: false,
		},
		{
			name: "both empty",
			config1: &ClusterConfig{
				Members: []ClusterMember{},
			},
			config2: &ClusterConfig{
				Members: []ClusterMember{},
			},
			expected: true,
		},
		{
			name: "same single member",
			config1: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node1", Address: "addr1", State: Voter},
				},
			},
			config2: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node1", Address: "addr1", State: Voter},
				},
			},
			expected: true,
		},
		{
			name: "different member count",
			config1: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node1", Address: "addr1", State: Voter},
				},
			},
			config2: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node1", Address: "addr1", State: Voter},
					{ID: "node2", Address: "addr2", State: NonVoter},
				},
			},
			expected: false,
		},
		{
			name: "different ID",
			config1: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node1", Address: "addr1", State: Voter},
				},
			},
			config2: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node2", Address: "addr1", State: Voter},
				},
			},
			expected: false,
		},
		{
			name: "different Address",
			config1: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node1", Address: "addr1", State: Voter},
				},
			},
			config2: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node1", Address: "addr2", State: Voter},
				},
			},
			expected: false,
		},
		{
			name: "different State",
			config1: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node1", Address: "addr1", State: Voter},
				},
			},
			config2: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node1", Address: "addr1", State: NonVoter},
				},
			},
			expected: false,
		},
		{
			name: "multiple members same",
			config1: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node1", Address: "addr1", State: Voter},
					{ID: "node2", Address: "addr2", State: NonVoter},
					{ID: "node3", Address: "addr3", State: Voter},
				},
			},
			config2: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node1", Address: "addr1", State: Voter},
					{ID: "node2", Address: "addr2", State: NonVoter},
					{ID: "node3", Address: "addr3", State: Voter},
				},
			},
			expected: true,
		},
		{
			name: "multiple members different order",
			config1: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node1", Address: "addr1", State: Voter},
					{ID: "node2", Address: "addr2", State: NonVoter},
				},
			},
			config2: &ClusterConfig{
				Members: []ClusterMember{
					{ID: "node2", Address: "addr2", State: NonVoter},
					{ID: "node1", Address: "addr1", State: Voter},
				},
			},
			expected: false, // Order matters
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config1.Equal(tt.config2)
			if result != tt.expected {
				t.Errorf("ClusterConfig.Equal() = %v, want %v", result, tt.expected)
			}
		})
	}
}
