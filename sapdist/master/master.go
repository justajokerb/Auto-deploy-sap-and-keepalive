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

func main() {
	flag.Parse()

	// Load existing metadata if available
	loadMetadata()

	// Start health checker daemon in background
	go startHealthChecker()

	// Setup API routers
	http.HandleFunc("/api/heartbeat", handleHeartbeat)
	http.HandleFunc("/api/status", handleStatus)
	http.HandleFunc("/api/files", handleFilesList)
	http.HandleFunc("/api/upload", handleUpload)
	http.HandleFunc("/api/download/", handleDownload)
	http.HandleFunc("/api/delete/", handleDelete)
	http.HandleFunc("/api/nodes/register", handleManualRegister)

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
	http.Handle("/", fs)

	p := *port
	if envPort := os.Getenv("PORT"); envPort != "" {
		fmt.Sscanf(envPort, "%d", &p)
	}
	addr := fmt.Sprintf(":%d", p)
	log.Printf("Master Controller listening on %s", addr)
	log.Printf("Serving console UI from: %s", staticDir)
	log.Printf("Metadata persistence file: %s", *metaFilePath)

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
			resp, err := client.Get(fmt.Sprintf("%s/status", worker.URL))
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

// selectBestWorker returns the online worker with the most free space
func selectBestWorker() *common.WorkerStats {
	metaLock.RLock()
	defer metaLock.RUnlock()

	var best *common.WorkerStats
	var maxFree int64 = -1

	for _, w := range metaData.Workers {
		if w.Status == "online" && w.FreeSpace > maxFree {
			maxFree = w.FreeSpace
			best = &w
		}
	}
	return best
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Limit multipart body to 500MB
	r.ParseMultipartForm(500 * 1024 * 1024)
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Failed to parse file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	fileName := header.Filename

	// Check if file already exists
	metaLock.RLock()
	_, exists := metaData.Files[fileName]
	metaLock.RUnlock()
	if exists {
		http.Error(w, "File already exists", http.StatusConflict)
		return
	}

	rand.Seed(time.Now().UnixNano())
	fileID := fmt.Sprintf("%d", rand.Int63())

	buffer := make([]byte, *chunkSize)
	var chunks []common.ChunkInfo
	var chunkIndex int

	log.Printf("Starting upload of %s (size: %d bytes)", fileName, header.Size)

	client := &http.Client{Timeout: 10 * time.Second}

	for {
		n, readErr := file.Read(buffer)
		if n > 0 {
			bestWorker := selectBestWorker()
			if bestWorker == nil {
				// Clean up uploaded chunks before failing
				for _, chk := range chunks {
					req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/chunks/%s", chk.WorkerURL, chk.ID), nil)
					client.Do(req)
				}
				http.Error(w, "No online storage workers available to store chunks", http.StatusServiceUnavailable)
				return
			}

			chunkID := fmt.Sprintf("%s_c%d", fileID, chunkIndex)
			chunkURL := fmt.Sprintf("%s/chunks/%s", bestWorker.URL, chunkID)

			// POST chunk to worker
			req, err := http.NewRequest(http.MethodPost, chunkURL, bytes.NewReader(buffer[:n]))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			req.Header.Set("Content-Type", "application/octet-stream")

			resp, err := client.Do(req)
			if err != nil || resp.StatusCode != http.StatusCreated {
				statusMsg := "unreachable"
				if resp != nil {
					statusMsg = fmt.Sprintf("status=%d", resp.StatusCode)
				}
				log.Printf("Failed to upload chunk %s to worker %s (%s)", chunkID, bestWorker.ID, statusMsg)
				// Clean up and abort
				for _, chk := range chunks {
					req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/chunks/%s", chk.WorkerURL, chk.ID), nil)
					client.Do(req)
				}
				http.Error(w, "Failed to upload chunk to storage node", http.StatusInternalServerError)
				return
			}
			resp.Body.Close()

			chunks = append(chunks, common.ChunkInfo{
				ID:        chunkID,
				Index:     chunkIndex,
				Size:      int64(n),
				WorkerID:  bestWorker.ID,
				WorkerURL: bestWorker.URL,
			})
			chunkIndex++

			// Lock and update worker free space estimate locally before actual status heartbeat syncs
			metaLock.Lock()
			wStats, ok := metaData.Workers[bestWorker.ID]
			if ok {
				wStats.UsedSpace += int64(n)
				wStats.FreeSpace -= int64(n)
				if wStats.FreeSpace < 0 {
					wStats.FreeSpace = 0
				}
				metaData.Workers[bestWorker.ID] = wStats
			}
			metaLock.Unlock()
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			http.Error(w, readErr.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Add file to metadata
	metaLock.Lock()
	metaData.Files[fileName] = common.FileMetadata{
		Name:       fileName,
		Size:       header.Size,
		UploadTime: time.Now(),
		Chunks:     chunks,
	}
	metaLock.Unlock()

	saveMetadata()
	log.Printf("Successfully uploaded file: %s split into %d chunks", fileName, len(chunks))

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("File uploaded successfully"))
}

func handleDownload(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 || parts[3] == "" {
		http.Error(w, "Invalid filename", http.StatusBadRequest)
		return
	}
	fileName := parts[3]

	metaLock.RLock()
	fileMeta, exists := metaData.Files[fileName]
	metaLock.RUnlock()

	if !exists {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", fileName))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fileMeta.Size))

	client := &http.Client{Timeout: 15 * time.Second}

	for _, chk := range fileMeta.Chunks {
		chunkURL := fmt.Sprintf("%s/chunks/%s", chk.WorkerURL, chk.ID)
		
		resp, err := client.Get(chunkURL)
		if err != nil || resp.StatusCode != http.StatusOK {
			statusMsg := "unreachable"
			if resp != nil {
				statusMsg = fmt.Sprintf("status=%d", resp.StatusCode)
			}
			log.Printf("Error: Fetching chunk %s from node %s failed: %v (%s)", chk.ID, chk.WorkerID, err, statusMsg)
			http.Error(w, fmt.Sprintf("Storage Node %s hosting file chunk is currently offline", chk.WorkerID), http.StatusServiceUnavailable)
			return
		}

		_, err = io.Copy(w, resp.Body)
		resp.Body.Close()
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

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 || parts[3] == "" {
		http.Error(w, "Invalid filename", http.StatusBadRequest)
		return
	}
	fileName := parts[3]

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
			chunkURL := fmt.Sprintf("%s/chunks/%s", chk.WorkerURL, chk.ID)
			req, err := http.NewRequest(http.MethodDelete, chunkURL, nil)
			if err == nil {
				resp, err := client.Do(req)
				if err == nil {
					resp.Body.Close()
				}
			}
		}
		log.Printf("Cleaned up chunks for deleted file: %s", fileName)
	}(fileMeta.Chunks)

	saveMetadata()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("File deleted"))
}
