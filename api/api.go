package api

type Response interface {
	Status() ResponseStatus
}

type PutRequest struct {
	Key   string
	Value string

	ClientID  int64
	RequestID int64
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
	ClientID  int64
	RequestID int64
}

type ListResponse struct {
	RespStatus ResponseStatus
	Pairs      map[string]string
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
