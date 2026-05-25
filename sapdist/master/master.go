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
	http.HandleFunc("/api/upload", requireBasicAuth(adminUser, adminPass, handleUpload))
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

	fileName := r.FormValue("relativePath")
	if fileName == "" {
		fileName = header.Filename
	}

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
			targetIDs := selectBestWorkerIDs(2) // Select up to 2 workers for RAID-1 redundancy
			if len(targetIDs) == 0 {
				// Clean up uploaded chunks before failing
				for _, chk := range chunks {
					for _, rep := range chk.Replicas {
						req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/chunks/%s", rep.WorkerURL, chk.ID), nil)
						if clusterToken != "" {
							req.Header.Set("Authorization", "Bearer "+clusterToken)
						}
						client.Do(req)
					}
				}
				http.Error(w, "No online storage workers available to store chunks", http.StatusServiceUnavailable)
				return
			}

			chunkID := fmt.Sprintf("%s_c%d", fileID, chunkIndex)
			var replicas []common.WorkerReplica

			// Write to all selected replica workers
			for _, wID := range targetIDs {
				metaLock.RLock()
				bestWorker := metaData.Workers[wID]
				metaLock.RUnlock()

				chunkURL := fmt.Sprintf("%s/chunks/%s", bestWorker.URL, chunkID)

				// POST chunk to worker
				req, err := http.NewRequest(http.MethodPost, chunkURL, bytes.NewReader(buffer[:n]))
				if err != nil {
					// Clean up previous chunks
					for _, chk := range chunks {
						for _, rep := range chk.Replicas {
							reqDel, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/chunks/%s", rep.WorkerURL, chk.ID), nil)
							if clusterToken != "" {
								reqDel.Header.Set("Authorization", "Bearer "+clusterToken)
							}
							client.Do(reqDel)
						}
					}
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
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
					}
					log.Printf("Failed to upload chunk %s to worker %s (%s)", chunkID, bestWorker.ID, statusMsg)
					// Clean up and abort
					for _, chk := range chunks {
						for _, rep := range chk.Replicas {
							reqDel, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/chunks/%s", rep.WorkerURL, chk.ID), nil)
							if clusterToken != "" {
								reqDel.Header.Set("Authorization", "Bearer "+clusterToken)
							}
							client.Do(reqDel)
						}
					}
					http.Error(w, "Failed to upload chunk replica to storage node", http.StatusInternalServerError)
					return
				}
				resp.Body.Close()

				replicas = append(replicas, common.WorkerReplica{
					WorkerID:  bestWorker.ID,
					WorkerURL: bestWorker.URL,
				})

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

			chunks = append(chunks, common.ChunkInfo{
				ID:       chunkID,
				Index:    chunkIndex,
				Size:     int64(n),
				Replicas: replicas,
			})
			chunkIndex++
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
	log.Printf("Successfully uploaded file: %s split into %d chunks with %d-way replication", fileName, len(chunks), len(chunks[0].Replicas))

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("File uploaded successfully"))
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
