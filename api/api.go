package api

type Response interface {
	Status() ResponseStatus
}

type PutRequest struct {
	Key   string
	Value string

	ClientID  int64
	RequestID int64
	LeaseID   int64
}

type PutResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	PrevValue  string
	Revision   int64
}

func (pr *PutResponse) Status() ResponseStatus {
	return pr.RespStatus
}

type AppendRequest struct {
	Key   string
	Value string

	ClientID  int64
	RequestID int64
}

type AppendResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	PrevValue  string
	Revision   int64
}

func (ar *AppendResponse) Status() ResponseStatus {
	return ar.RespStatus
}

type GetRequest struct {
	Key string

	ClientID  int64
	RequestID int64
}

type GetResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	Value      string
	Revision   int64
}

func (gr *GetResponse) Status() ResponseStatus {
	return gr.RespStatus
}

type CASRequest struct {
	Key          string
	CompareValue string
	Value        string

	ClientID  int64
	RequestID int64
}

type CASResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	PrevValue  string
	Revision   int64
}

func (cr *CASResponse) Status() ResponseStatus {
	return cr.RespStatus
}

type ResponseStatus int

const (
	StatusInvalid ResponseStatus = iota
	StatusOK
	StatusNotLeader
	StatusFailedCommit
	StatusDuplicateRequest
)

var responseName = map[ResponseStatus]string{
	StatusInvalid:          "invalid",
	StatusOK:               "OK",
	StatusNotLeader:        "NotLeader",
	StatusFailedCommit:     "FailedCommit",
	StatusDuplicateRequest: "DuplicateRequest",
}

type ListRequest struct {
	Prefix    string
	Limit     int
	KeyAfter  string
	ClientID  int64
	RequestID int64
}

type ListResponse struct {
	RespStatus ResponseStatus
	Pairs      map[string]string
	NextKey    string
	Revision   int64
}

func (lr *ListResponse) Status() ResponseStatus {
	return lr.RespStatus
}

func (rs ResponseStatus) String() string {
	return responseName[rs]
}

type JoinRequest struct {
	ID       int    `json:"id"`
	RaftAddr string `json:"raft_addr"`
	HTTPAddr string `json:"http_addr"`
}

type JoinResponse struct {
	RespStatus ResponseStatus `json:"resp_status"`
}

func (jr *JoinResponse) Status() ResponseStatus { return jr.RespStatus }

type LeaveRequest struct {
	ID int `json:"id"`
}

type LeaveResponse struct {
	RespStatus ResponseStatus `json:"resp_status"`
}

func (lr *LeaveResponse) Status() ResponseStatus { return lr.RespStatus }

type PeerInfo struct {
	ID       int    `json:"id"`
	HTTPAddr string `json:"http_addr"`
}

type StatusResponse struct {
	RespStatus ResponseStatus `json:"resp_status"`
	ID         int            `json:"id"`
	IsLeader   bool           `json:"is_leader"`
	PeerCount  int            `json:"peer_count"`
}

func (sr *StatusResponse) Status() ResponseStatus { return sr.RespStatus }

type MembersResponse struct {
	RespStatus ResponseStatus `json:"resp_status"`
	Members    []PeerInfo     `json:"members"`
}

func (mr *MembersResponse) Status() ResponseStatus { return mr.RespStatus }

type WatchEventType int

const (
	EventInvalid WatchEventType = iota
	EventPut
	EventDelete
)

var WatchEventName = map[WatchEventType]string{
	EventPut:    "put",
	EventDelete: "delete",
}

func (et WatchEventType) String() string { return WatchEventName[et] }

var WatchEventTypeFromName = map[string]WatchEventType{
	"put":    EventPut,
	"delete": EventDelete,
}

type WatchEvent struct {
	Key      string         `json:"key"`
	Value    string         `json:"value,omitempty"`
	Revision int64          `json:"revision"`
	Type     WatchEventType `json:"-"`
}

type CompareOp int

const (
	CompareEQ      CompareOp = iota
	CompareNEQ
	CompareGTE
	CompareLTE
	CompareExists
	CompareNotExists
)

type TxnOpType int

const (
	TxnOpPut    TxnOpType = iota
	TxnOpDelete
	TxnOpCAS
	TxnOpAppend
)

type TxnCondition struct {
	Key     string    `json:"key"`
	Compare CompareOp `json:"compare"`
	Value   string    `json:"value"`
}

type TxnOp struct {
	Op           TxnOpType `json:"op"`
	Key          string    `json:"key"`
	Value        string    `json:"value,omitempty"`
	CompareValue string    `json:"compare_value,omitempty"`
	LeaseID      int64     `json:"lease_id,omitempty"`
}

type TxnRequest struct {
	Conditions []TxnCondition `json:"conditions"`
	Success    []TxnOp        `json:"success"`
	Failure    []TxnOp        `json:"failure"`
	ClientID   int64          `json:"clientID"`
	RequestID  int64          `json:"requestID"`
}

type TxnOpResult struct {
	Key       string `json:"key"`
	PrevValue string `json:"prev_value,omitempty"`
	KeyFound  bool   `json:"key_found"`
}

type TxnResponse struct {
	RespStatus ResponseStatus `json:"resp_status"`
	Succeeded  bool           `json:"succeeded"`
	Results    []TxnOpResult  `json:"results"`
	Revision   int64          `json:"revision"`
}

func (tr *TxnResponse) Status() ResponseStatus { return tr.RespStatus }

type LeaseGrantRequest struct {
	TTL       int64 `json:"ttl"`
	ClientID  int64 `json:"clientID"`
	RequestID int64 `json:"requestID"`
}

type LeaseGrantResponse struct {
	RespStatus ResponseStatus `json:"resp_status"`
	ID         int64          `json:"id"`
	TTL        int64          `json:"ttl"`
}

func (lr *LeaseGrantResponse) Status() ResponseStatus { return lr.RespStatus }

type LeaseKeepAliveRequest struct {
	ID        int64 `json:"id"`
	ClientID  int64 `json:"clientID"`
	RequestID int64 `json:"requestID"`
}

type LeaseKeepAliveResponse struct {
	RespStatus ResponseStatus `json:"resp_status"`
	ID         int64          `json:"id"`
}

func (lkr *LeaseKeepAliveResponse) Status() ResponseStatus { return lkr.RespStatus }

type LeaseRevokeRequest struct {
	ID        int64 `json:"id"`
	ClientID  int64 `json:"clientID"`
	RequestID int64 `json:"requestID"`
}

type LeaseRevokeResponse struct {
	RespStatus ResponseStatus `json:"resp_status"`
}

func (lrr *LeaseRevokeResponse) Status() ResponseStatus { return lrr.RespStatus }
