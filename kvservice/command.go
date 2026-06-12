package kvservice

import "github.com/MHS-20/Zodiac/api"

type Command struct {
	Kind CommandKind

	Key, Value string

	CompareValue string

	ResultValue string
	ResultFound bool

	ResultPairs map[string]string

	Revision int64

	Txn     *TxnData
	TxnResult *TxnApplyResult

	LeaseID  int64
	LeaseTTL int64
	LeaseIDs []int64

	Limit    int
	KeyAfter string

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
	CommandList
	CommandTxn
	CommandLeaseGrant
	CommandLeaseKeepAlive
	CommandLeaseRevoke
	CommandLeaseExpiry
)

var commandName = map[CommandKind]string{
	CommandInvalid:       "invalid",
	CommandGet:           "get",
	CommandPut:           "put",
	CommandAppend:        "append",
	CommandCAS:           "cas",
	CommandList:          "list",
	CommandTxn:           "txn",
	CommandLeaseGrant:    "lease-grant",
	CommandLeaseKeepAlive: "lease-keepalive",
	CommandLeaseRevoke:   "lease-revoke",
	CommandLeaseExpiry:   "lease-expiry",
}

func (ck CommandKind) String() string {
	return commandName[ck]
}

type TxnData struct {
	Conditions []api.TxnCondition
	Success    []api.TxnOp
	Failure    []api.TxnOp
}

type TxnApplyResult struct {
	Succeeded bool
	Results   []api.TxnOpResult
}
