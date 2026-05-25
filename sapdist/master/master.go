package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"sapdist/common"
)

type Metadata struct {
	Files   map[string]common.FileMetadata `json:"files"`
	Workers map[string]common.WorkerStats  `json:"workers"`
}

var (
	port         = flag.Int("port", 8080, "Port to run the Master controller on")
	metaFilePath = flag.String("meta", "./master_metadata.json", "Metadata JSON persistence file path")
	chunkSize    = flag.Int64("chunksize", 2*1024*1024, "Size of file chunks in bytes (default 2MB)")
)

var (
	metaLock sync.RWMutex
	metaData Metadata
)

func init() {
	metaData.Files = make(map[string]common.FileMetadata)
	metaData.Workers = make(map[string]common.WorkerStats)
}

var (
	adminUser    = "admin"
	adminPass    = ""
	clusterToken = ""
)

func requireBasicAuth(username, password string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if username != "" && password != "" {
			user, pass, ok := r.BasicAuth()
			if !ok || user != username || pass != password {
				w.Header().Set("WWW-Authenticate", `Basic realm="SAP Unified Storage"`)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}

func requireClusterToken(secret string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if secret != "" {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				http.Error(w, "Unauthorized: Missing Authorization header", http.StatusUnauthorized)
				return
			}
			parts := strings.Split(authHeader, " ")
			if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" || parts[1] != secret {
				http.Error(w, "Unauthorized: Invalid cluster token", http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}

func main() {
	flag.Parse()

	// Load credentials from environment
	if envUser := os.Getenv("ADMIN_USERNAME"); envUser != "" {
		adminUser = envUser
	}
	if envPass := os.Getenv("ADMIN_PASSWORD"); envPass != "" {
		adminPass = envPass
	}
	if envToken := os.Getenv("CLUSTER_SECRET"); envToken != "" {
		clusterToken = envToken
	}

	// Load existing metadata if available
	loadMetadata()

	// Start health checker daemon in background
	go startHealthChecker()

	// Setup API routers
	http.HandleFunc("/api/heartbeat", requireClusterToken(clusterToken, handleHeartbeat))
	http.HandleFunc("/api/status", requireBasicAuth(adminUser, adminPass, handleStatus))
	http.HandleFunc("/api/files", requireBasicAuth(adminUser, adminPass, handleFilesList))
	http.HandleFunc("/api/upload/start", requireBasicAuth(adminUser, adminPass, handleUploadStart))
	http.HandleFunc("/api/upload/chunk", requireBasicAuth(adminUser, adminPass, handleUploadChunk))
	http.HandleFunc("/api/upload/finish", requireBasicAuth(adminUser, adminPass, handleUploadFinish))
	http.HandleFunc("/api/download/", requireBasicAuth(adminUser, adminPass, handleDownload))
	http.HandleFunc("/api/delete/", requireBasicAuth(adminUser, adminPass, handleDelete))
	http.HandleFunc("/api/nodes/register", requireBasicAuth(adminUser, adminPass, handleManualRegister))

	// Determine static files directory
	staticDir := "./static"
	if _, err := os.Stat(staticDir); err != nil {
		staticDir = "./master/static"
	}
	if _, err := os.Stat(staticDir); err != nil {
		// Create the static dir if missing so server runs anyway
		os.MkdirAll(staticDir, 0755)
	}

	// Serve Static UI Console
	fs := http.FileServer(http.Dir(staticDir))
	http.Handle("/", requireBasicAuth(adminUser, adminPass, fs.ServeHTTP))

	p := *port
	if envPort := os.Getenv("PORT"); envPort != "" {
		fmt.Sscanf(envPort, "%d", &p)
	}
	addr := fmt.Sprintf(":%d", p)
	log.Printf("Master Controller listening on %s", addr)
	log.Printf("Serving console UI from: %s", staticDir)
	log.Printf("Metadata persistence file: %s", *metaFilePath)
	if adminPass != "" {
		log.Printf("Admin UI security: enabled (user: %s)", adminUser)
	} else {
		log.Printf("Admin UI security: disabled (unsecured)")
	}
	if clusterToken != "" {
		log.Printf("Cluster node security: enabled")
	} else {
		log.Printf("Cluster node security: disabled (unsecured)")
	}

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Master server failed: %v", err)
	}
}

func loadMetadata() {
	metaLock.Lock()
	defer metaLock.Unlock()

	data, err := os.ReadFile(*metaFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("No existing metadata file found. Starting fresh.")
			return
		}
		log.Printf("Error reading metadata file: %v", err)
		return
	}

	var temp Metadata
	if err := json.Unmarshal(data, &temp); err != nil {
		log.Printf("Error unmarshalling metadata: %v", err)
		return
	}

	if temp.Files != nil {
		metaData.Files = temp.Files
	}
	if temp.Workers != nil {
		metaData.Workers = temp.Workers
	}
	log.Printf("Loaded metadata: %d files, %d workers.", len(metaData.Files), len(metaData.Workers))
}

func saveMetadata() {
	metaLock.RLock()
	defer metaLock.RUnlock()

	data, err := json.MarshalIndent(metaData, "", "  ")
	if err != nil {
		log.Printf("Error marshalling metadata for save: %v", err)
		return
	}

	err = os.WriteFile(*metaFilePath, data, 0644)
	if err != nil {
		log.Printf("Error writing metadata file: %v", err)
	}
}

func startHealthChecker() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	client := &http.Client{Timeout: 3 * time.Second}

	for range ticker.C {
		metaLock.Lock()
		for id, worker := range metaData.Workers {
			// Check if heartbeat is older than 30s
			if time.Since(worker.LastHeartbeat) > 30*time.Second {
				if worker.Status != "offline" {
					log.Printf("Node %s (%s) went offline (heartbeat timeout)", id, worker.URL)
				}
				worker.Status = "offline"
				worker.LatencyMs = -1
				metaData.Workers[id] = worker
				continue
			}

			// Active ping to measure latency and update storage details
			start := time.Now()
			req, err := http.NewRequest("GET", fmt.Sprintf("%s/status", worker.URL), nil)
			if err != nil {
				if worker.Status != "offline" {
					log.Printf("Node %s (%s) unreachable: %v", id, worker.URL, err)
				}
				worker.Status = "offline"
				worker.LatencyMs = -1
				metaData.Workers[id] = worker
				continue
			}
			if clusterToken != "" {
				req.Header.Set("Authorization", "Bearer "+clusterToken)
			}

			resp, err := client.Do(req)
			if err != nil {
				if worker.Status != "offline" {
					log.Printf("Node %s (%s) unreachable: %v", id, worker.URL, err)
				}
				worker.Status = "offline"
				worker.LatencyMs = -1
				metaData.Workers[id] = worker
				continue
			}
			latency := time.Since(start).Milliseconds()

			var stats common.HeartbeatRequest
			err = json.NewDecoder(resp.Body).Decode(&stats)
			resp.Body.Close()
			if err == nil {
				worker.TotalSpace = stats.TotalSpace
				worker.FreeSpace = stats.FreeSpace
				worker.UsedSpace = stats.UsedSpace
			}

			worker.Status = "online"
			worker.LatencyMs = latency
			metaData.Workers[id] = worker
		}
		metaLock.Unlock()

		// Save state periodically
		saveMetadata()
	}
}

func handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req common.HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	metaLock.Lock()
	worker, exists := metaData.Workers[req.ID]
	if !exists {
		log.Printf("Registering new Worker Node: %s at %s", req.ID, req.URL)
		worker = common.WorkerStats{
			ID:  req.ID,
			URL: req.URL,
		}
	}
	worker.TotalSpace = req.TotalSpace
	worker.FreeSpace = req.FreeSpace
	worker.UsedSpace = req.UsedSpace
	worker.LastHeartbeat = time.Now()
	worker.Status = "online"
	metaData.Workers[req.ID] = worker
	metaLock.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(common.HeartbeatResponse{Success: true, Message: "Heartbeat recorded"})
}

func handleManualRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ID  string `json:"id"`
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" || req.URL == "" {
		http.Error(w, "Invalid inputs", http.StatusBadRequest)
		return
	}

	metaLock.Lock()
	metaData.Workers[req.ID] = common.WorkerStats{
		ID:            req.ID,
		URL:           req.URL,
		LastHeartbeat: time.Now(),
		Status:        "online",
	}
	metaLock.Unlock()

	saveMetadata()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Node registered manually"))
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	metaLock.RLock()
	defer metaLock.RUnlock()

	var totalPool, freePool, usedPool int64
	var nodes []common.WorkerStats

	for _, w := range metaData.Workers {
		nodes = append(nodes, w)
		if w.Status == "online" {
			totalPool += w.TotalSpace
			freePool += w.FreeSpace
			usedPool += w.UsedSpace
		}
	}

	// Sort nodes alphabetically ascending by ID
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total_space": totalPool,
		"free_space":  freePool,
		"used_space":  usedPool,
		"nodes":       nodes,
	})
}

func handleFilesList(w http.ResponseWriter, r *http.Request) {
	metaLock.RLock()
	defer metaLock.RUnlock()

	var files []common.FileMetadata
	for _, f := range metaData.Files {
		files = append(files, f)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(files)
}

// selectBestWorkerIDs returns the online worker IDs sorted by free space descending
func selectBestWorkerIDs(count int) []string {
	metaLock.RLock()
	defer metaLock.RUnlock()

	var onlineWorkers []common.WorkerStats
	for _, w := range metaData.Workers {
		if w.Status == "online" {
			onlineWorkers = append(onlineWorkers, w)
		}
	}

	// Sort by FreeSpace descending
	sort.Slice(onlineWorkers, func(i, j int) bool {
		return onlineWorkers[i].FreeSpace > onlineWorkers[j].FreeSpace
	})

	var ids []string
	for i := 0; i < len(onlineWorkers) && i < count; i++ {
		ids = append(ids, onlineWorkers[i].ID)
	}
	return ids
}

type UploadSession struct {
	FileID       uint64
	FileName     string
	Size         int64
	TotalChunks  int
	UploadedInfo []common.ChunkInfo // stores replica details for each chunk
	Mu           sync.Mutex
}

var (
	activeUploads     = make(map[string]*UploadSession)
	activeUploadsLock sync.RWMutex
)

type StartUploadRequest struct {
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	TotalChunks int    `json:"totalChunks"`
}

type StartUploadResponse struct {
	UploadID string `json:"uploadId"`
}

func handleUploadStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req StartUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Filename == "" || req.TotalChunks <= 0 {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Check if file already exists
	metaLock.RLock()
	_, exists := metaData.Files[req.Filename]
	metaLock.RUnlock()
	if exists {
		http.Error(w, "File already exists", http.StatusConflict)
		return
	}

	rand.Seed(time.Now().UnixNano())
	fileID := rand.Uint64()
	uploadID := fmt.Sprintf("%d", fileID)

	session := &UploadSession{
		FileID:       fileID,
		FileName:     req.Filename,
		Size:         req.Size,
		TotalChunks:  req.TotalChunks,
		UploadedInfo: make([]common.ChunkInfo, req.TotalChunks),
	}

	activeUploadsLock.Lock()
	activeUploads[uploadID] = session
	activeUploadsLock.Unlock()

	log.Printf("Starting chunked upload session %s for file %s (size: %d bytes, chunks: %d)", uploadID, req.Filename, req.Size, req.TotalChunks)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(StartUploadResponse{UploadID: uploadID})
}

func handleUploadChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse multipart body
	r.ParseMultipartForm(10 * 1024 * 1024) // 10MB chunk max limit

	uploadID := r.FormValue("uploadId")
	chunkIndexStr := r.FormValue("chunkIndex")
	
	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Failed to read chunk file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	var chunkIndex int
	if _, err := fmt.Sscanf(chunkIndexStr, "%d", &chunkIndex); err != nil {
		http.Error(w, "Invalid chunkIndex", http.StatusBadRequest)
		return
	}

	activeUploadsLock.RLock()
	session, exists := activeUploads[uploadID]
	activeUploadsLock.RUnlock()
	if !exists {
		http.Error(w, "Upload session not found or expired", http.StatusNotFound)
		return
	}

	if chunkIndex < 0 || chunkIndex >= session.TotalChunks {
		http.Error(w, "Chunk index out of bounds", http.StatusBadRequest)
		return
	}

	// Read entire chunk into memory (typically 2MB)
	chunkBytes, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "Failed to read chunk data: "+err.Error(), http.StatusInternalServerError)
		return
	}
	chunkSizeVal := int64(len(chunkBytes))

	targetIDs := selectBestWorkerIDs(2) // Select up to 2 workers for RAID-1 redundancy
	if len(targetIDs) == 0 {
		http.Error(w, "No online storage workers available to store chunks", http.StatusServiceUnavailable)
		return
	}

	chunkID := fmt.Sprintf("%d_c%d", session.FileID, chunkIndex)
	var replicas []common.WorkerReplica

	client := &http.Client{Timeout: 15 * time.Second}

	// Upload to all target workers
	for _, wID := range targetIDs {
		metaLock.RLock()
		worker, ok := metaData.Workers[wID]
		metaLock.RUnlock()
		if !ok || worker.Status != "online" {
			continue
		}

		chunkURL := fmt.Sprintf("%s/chunks/%s", worker.URL, chunkID)
		req, err := http.NewRequest(http.MethodPost, chunkURL, bytes.NewReader(chunkBytes))
		if err != nil {
			log.Printf("[%s] Failed to create request for worker %s: %v", chunkID, wID, err)
			continue
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		if clusterToken != "" {
			req.Header.Set("Authorization", "Bearer "+clusterToken)
		}

		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusCreated {
			statusMsg := "unreachable"
			if resp != nil {
				statusMsg = fmt.Sprintf("status=%d", resp.StatusCode)
				resp.Body.Close()
			}
			log.Printf("[%s] Failed to upload chunk to worker %s (%s)", chunkID, wID, statusMsg)
			continue
		}
		resp.Body.Close()

		replicas = append(replicas, common.WorkerReplica{
			WorkerID:  worker.ID,
			WorkerURL: worker.URL,
		})

		// Lock and update worker free space estimate locally before actual status heartbeat syncs
		metaLock.Lock()
		wStats, ok := metaData.Workers[worker.ID]
		if ok {
			wStats.UsedSpace += chunkSizeVal
			wStats.FreeSpace -= chunkSizeVal
			if wStats.FreeSpace < 0 {
				wStats.FreeSpace = 0
			}
			metaData.Workers[worker.ID] = wStats
		}
		metaLock.Unlock()
	}

	if len(replicas) == 0 {
		http.Error(w, "Failed to upload chunk replica to any storage node", http.StatusInternalServerError)
		return
	}

	session.Mu.Lock()
	session.UploadedInfo[chunkIndex] = common.ChunkInfo{
		ID:       chunkID,
		Index:    chunkIndex,
		Size:     chunkSizeVal,
		Replicas: replicas,
	}
	session.Mu.Unlock()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

type FinishUploadRequest struct {
	UploadID string `json:"uploadId"`
}

func handleUploadFinish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FinishUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UploadID == "" {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	activeUploadsLock.Lock()
	session, exists := activeUploads[req.UploadID]
	if exists {
		delete(activeUploads, req.UploadID)
	}
	activeUploadsLock.Unlock()

	if !exists {
		http.Error(w, "Upload session not found", http.StatusNotFound)
		return
	}

	// Verify all chunks are present
	for idx, chunk := range session.UploadedInfo {
		if len(chunk.Replicas) == 0 {
			http.Error(w, fmt.Sprintf("Missing or failed chunk index %d", idx), http.StatusInternalServerError)
			return
		}
	}

	// Save to metadata catalog
	metaLock.Lock()
	metaData.Files[session.FileName] = common.FileMetadata{
		Name:       session.FileName,
		Size:       session.Size,
		UploadTime: time.Now(),
		Chunks:     session.UploadedInfo,
	}
	metaLock.Unlock()

	saveMetadata()
	log.Printf("Successfully completed chunked upload: %s (size: %d, chunks: %d)", session.FileName, session.Size, session.TotalChunks)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success"}`))
}

func handleDownload(w http.ResponseWriter, r *http.Request) {
	fileName := strings.TrimPrefix(r.URL.Path, "/api/download/")
	if fileName == "" {
		http.Error(w, "Invalid filename", http.StatusBadRequest)
		return
	}

	metaLock.RLock()
	fileMeta, exists := metaData.Files[fileName]
	metaLock.RUnlock()

	if !exists {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Use safe display filename for attachment header
	safeName := fileName
	if slashIdx := strings.LastIndex(fileName, "/"); slashIdx != -1 {
		safeName = fileName[slashIdx+1:]
	}

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", safeName))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fileMeta.Size))

	client := &http.Client{Timeout: 15 * time.Second}

	for _, chk := range fileMeta.Chunks {
		var chunkData []byte
		var fetchErr error

		// Try downloading the chunk from any of the replicas
		for _, rep := range chk.Replicas {
			chunkURL := fmt.Sprintf("%s/chunks/%s", rep.WorkerURL, chk.ID)
			req, err := http.NewRequest("GET", chunkURL, nil)
			if err != nil {
				fetchErr = err
				continue
			}
			if clusterToken != "" {
				req.Header.Set("Authorization", "Bearer "+clusterToken)
			}

			resp, err := client.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				chunkData, fetchErr = io.ReadAll(resp.Body)
				resp.Body.Close()
				if fetchErr == nil {
					break // Successfully read chunk data from this replica
				}
			} else {
				if resp != nil {
					resp.Body.Close()
				}
				statusMsg := "unreachable"
				if resp != nil {
					statusMsg = fmt.Sprintf("status=%d", resp.StatusCode)
				}
				fetchErr = fmt.Errorf("worker %s failed: %v (%s)", rep.WorkerID, err, statusMsg)
			}
		}

		if len(chunkData) == 0 || fetchErr != nil {
			log.Printf("Error: All replicas for chunk %s failed: %v", chk.ID, fetchErr)
			http.Error(w, fmt.Sprintf("All storage nodes hosting file chunk %s are offline", chk.ID), http.StatusServiceUnavailable)
			return
		}

		_, err := io.Copy(w, bytes.NewReader(chunkData))
		if err != nil {
			log.Printf("Error streaming chunk %s to client: %v", chk.ID, err)
			return
		}
	}
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	fileName := strings.TrimPrefix(r.URL.Path, "/api/delete/")
	if fileName == "" {
		http.Error(w, "Invalid filename", http.StatusBadRequest)
		return
	}

	metaLock.Lock()
	fileMeta, exists := metaData.Files[fileName]
	if !exists {
		metaLock.Unlock()
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	delete(metaData.Files, fileName)
	metaLock.Unlock()

	// Deletes chunks in background (best effort)
	go func(chunks []common.ChunkInfo) {
		client := &http.Client{Timeout: 5 * time.Second}
		for _, chk := range chunks {
			for _, rep := range chk.Replicas {
				chunkURL := fmt.Sprintf("%s/chunks/%s", rep.WorkerURL, chk.ID)
				req, err := http.NewRequest(http.MethodDelete, chunkURL, nil)
				if err == nil {
					if clusterToken != "" {
						req.Header.Set("Authorization", "Bearer "+clusterToken)
					}
					resp, err := client.Do(req)
					if err == nil {
						resp.Body.Close()
					}
				}
			}
		}
		log.Printf("Cleaned up replica chunks for deleted file: %s", fileName)
	}(fileMeta.Chunks)

	saveMetadata()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("File deleted"))
}
