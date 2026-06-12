package kvservice

type Command struct {
	Kind CommandKind

	Key, Value string

	CompareValue string

	ResultValue string
	ResultFound bool

	ResultPairs map[string]string

	ServiceID           int
	ClientID, RequestID int64
	IsDuplicate         bool
	Revision            int64
}

type CommandKind int

const (
	CommandInvalid CommandKind = iota
	CommandGet
	CommandPut
	CommandAppend
	CommandCAS
	CommandList
)

var commandName = map[CommandKind]string{
	CommandInvalid: "invalid",
	CommandGet:     "get",
	CommandPut:     "put",
	CommandAppend:  "append",
	CommandCAS:     "cas",
	CommandList:    "list",
}

func (ck CommandKind) String() string {
	return commandName[ck]
}
