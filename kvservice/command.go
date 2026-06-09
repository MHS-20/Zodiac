package kvservice

type Command struct {
	Kind CommandKind

	Key, Value string

	CompareValue string

	ResultValue string
	ResultFound bool

	ServiceID           int
	ClientID, RequestID int64
	IsDuplicate         bool
}

type CommandKind int

const (
	CommandInvalid CommandKind = iota
	CommandGet
	CommandPut
	CommandAppend
	CommandCAS
)

var commandName = map[CommandKind]string{
	CommandInvalid: "invalid",
	CommandGet:     "get",
	CommandPut:     "put",
	CommandAppend:  "append",
	CommandCAS:     "cas",
}

func (ck CommandKind) String() string {
	return commandName[ck]
}
