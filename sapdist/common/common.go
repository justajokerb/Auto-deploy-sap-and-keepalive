package common

import "time"

type WorkerStats struct {
	ID            string    `json:"id"`
	URL           string    `json:"url"`
	TotalSpace    int64     `json:"total_space"`
	FreeSpace     int64     `json:"free_space"`
	UsedSpace     int64     `json:"used_space"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Status        string    `json:"status"` // "online" or "offline"
	LatencyMs     int64     `json:"latency_ms"`
}

type ChunkInfo struct {
	ID        string `json:"id"`
	Index     int    `json:"index"`
	Size      int64  `json:"size"`
	WorkerID  string `json:"worker_id"`
	WorkerURL string `json:"worker_url"`
}

type FileMetadata struct {
	Name       string      `json:"name"`
	Size       int64       `json:"size"`
	UploadTime time.Time   `json:"upload_time"`
	Chunks     []ChunkInfo `json:"chunks"`
}

type HeartbeatRequest struct {
	ID         string `json:"id"`
	URL        string `json:"url"`
	TotalSpace int64  `json:"total_space"`
	FreeSpace  int64  `json:"free_space"`
	UsedSpace  int64  `json:"used_space"`
}

type HeartbeatResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}
