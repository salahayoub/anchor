package tui

import (
	"errors"
	"strings"
)

// CommandType represents the type of command.
type CommandType int

const (
	CommandGet CommandType = iota
	CommandPut
)

// String returns a human-readable representation of the CommandType.
func (c CommandType) String() string {
	switch c {
	case CommandGet:
		return "GET"
	case CommandPut:
		return "PUT"
	default:
		return "UNKNOWN"
	}
}

// Command represents a parsed command with its operation, key, and optional value.
type Command struct {
	Type  CommandType
	Key   string
	Value string // Only used for PUT commands
}

// Common parsing errors.
var (
	ErrEmptyCommand   = errors.New("empty command")
	ErrUnknownCommand = errors.New("unknown command: expected GET or PUT")
	ErrMissingKey     = errors.New("missing key")
	ErrMissingValue   = errors.New("missing value for PUT command")
)

// ParseCommand parses a command string and returns a structured Command.
// Supported syntax:
//   - "GET key" - retrieves the value for the given key
//   - "PUT key value" - sets the key to the given value
//
// Returns an error for invalid command syntax.
func ParseCommand(input string) (*Command, error) {
	// Trim whitespace
	input = strings.TrimSpace(input)

	if input == "" {
		return nil, ErrEmptyCommand
	}

	// Split into parts
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return nil, ErrEmptyCommand
	}

	// Parse command type (case-insensitive)
	cmdType := strings.ToUpper(parts[0])

	switch cmdType {
	case "GET":
		if len(parts) < 2 {
			return nil, ErrMissingKey
		}
		return &Command{
			Type: CommandGet,
			Key:  parts[1],
		}, nil

	case "PUT":
		if len(parts) < 2 {
			return nil, ErrMissingKey
		}
		if len(parts) < 3 {
			return nil, ErrMissingValue
		}
		// Join remaining parts as value (allows spaces in value)
		value := strings.Join(parts[2:], " ")
		return &Command{
			Type:  CommandPut,
			Key:   parts[1],
			Value: value,
		}, nil

	default:
		return nil, ErrUnknownCommand
	}
}
