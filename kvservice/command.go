package kvservice

// Command is the concrete command type KVService submits to the Raft log to
// manage its state machine. It's also used to carry the results of the command
// after it's applied to the state machine.
type Command struct {
	Kind CommandKind

	Key, Value string

	CompareValue string

	ResultValue string
	ResultFound bool

	// id is the Raft ID of the server submitting this command.
	Id int
}

type CommandKind int

const (
	CommandInvalid CommandKind = iota
	CommandGet
	CommandPut
	CommandCAS
)

var commandName = map[CommandKind]string{
	CommandInvalid: "invalid",
	CommandGet:     "get",
	CommandPut:     "put",
	CommandCAS:     "cas",
}

func (ck CommandKind) String() string {
	return commandName[ck]
}
