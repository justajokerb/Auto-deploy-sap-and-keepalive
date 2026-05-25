package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sapdist/common"
)

var (
	port       = flag.Int("port", 8081, "Port to run the worker HTTP server on")
	storageDir = flag.String("dir", "./storage", "Directory to store chunk files")
	quotaGB    = flag.Float64("quota", 10.0, "Total allocated storage quota in GB")
	masterURL  = flag.String("master", "http://localhost:8080", "URL of the Master controller")
	workerID   = flag.String("id", "", "Unique worker ID (randomly generated if empty)")
	externalURL = flag.String("url", "", "External accessible URL of this worker (auto-derived if empty)")
)

func main() {
	flag.Parse()

	// Initialize random generator seed
	rand.Seed(time.Now().UnixNano())

	// Generate random worker ID if not specified, fallback to env WORKER_ID
	id := *workerID
	if id == "" {
		id = os.Getenv("WORKER_ID")
	}
	if id == "" {
		id = fmt.Sprintf("worker-%d", rand.Intn(10000))
	}

	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(*storageDir, 0755); err != nil {
		log.Fatalf("Failed to create storage directory: %v", err)
	}

	// Determine port, fallback to env PORT
	p := *port
	if envPort := os.Getenv("PORT"); envPort != "" {
		fmt.Sscanf(envPort, "%d", &p)
	}

	// Determine external URL, fallback to env WORKER_URL
	urlStr := *externalURL
	if urlStr == "" {
		urlStr = os.Getenv("WORKER_URL")
	}
	if urlStr == "" {
		// Default to localhost
		urlStr = fmt.Sprintf("http://localhost:%d", p)
	}
	// Ensure URL has prefix
	if !strings.HasPrefix(urlStr, "http://") && !strings.HasPrefix(urlStr, "https://") {
		urlStr = "http://" + urlStr
	}

	// Determine master URL, fallback to env MASTER_URL
	mURL := *masterURL
	if envMURL := os.Getenv("MASTER_URL"); envMURL != "" {
		mURL = envMURL
	}

	log.Printf("Starting Worker Node %s", id)
	log.Printf("Storage Directory: %s", *storageDir)
	log.Printf("Quota limit: %.2f GB", *quotaGB)
	log.Printf("Master Controller: %s", mURL)
	log.Printf("Node Access URL: %s", urlStr)

	// Start heartbeat loop in background if master URL is provided
	if mURL != "" {
		go startHeartbeatLoop(id, urlStr, mURL)
	}

	// Define HTTP handlers
	http.HandleFunc("/chunks/", handleChunk(id))
	http.HandleFunc("/status", handleStatus)

	addr := fmt.Sprintf(":%d", p)
	log.Printf("Worker listening on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server exited with error: %v", err)
	}
}

// getStorageStats calculates used space by scanning the files in storage folder
func getStorageStats() (total, free, used int64) {
	total = int64(*quotaGB * 1024 * 1024 * 1024)

	err := filepath.Walk(*storageDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			used += info.Size()
		}
		return nil
	})
	if err != nil {
		log.Printf("Warning: Failed to walk storage dir: %v", err)
	}

	free = total - used
	if free < 0 {
		free = 0
	}
	return
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	total, free, used := getStorageStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(common.HeartbeatRequest{
		TotalSpace: total,
		FreeSpace:  free,
		UsedSpace:  used,
	})
}

func handleChunk(wID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract chunk ID from path (e.g. /chunks/some-chunk-id)
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 3 || parts[2] == "" {
			http.Error(w, "Invalid chunk ID", http.StatusBadRequest)
			return
		}
		chunkID := parts[2]
		filePath := filepath.Join(*storageDir, chunkID)

		switch r.Method {
		case http.MethodGet:
			// Read and stream chunk file
			file, err := os.Open(filePath)
			if err != nil {
				if os.IsNotExist(err) {
					http.Error(w, "Chunk not found", http.StatusNotFound)
				} else {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return
			}
			defer file.Close()

			w.Header().Set("Content-Type", "application/octet-stream")
			io.Copy(w, file)
			log.Printf("[%s] Served chunk: %s", wID, chunkID)

		case http.MethodPost:
			// Write request body to chunk file
			_, _, used := getStorageStats()
			total := int64(*quotaGB * 1024 * 1024 * 1024)
			
			// Enforce quota limit (check if request content length fits)
			if r.ContentLength > 0 && used+r.ContentLength > total {
				http.Error(w, "Quota exceeded", http.StatusRequestEntityTooLarge)
				return
			}

			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer file.Close()

			written, err := io.Copy(file, r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			log.Printf("[%s] Saved chunk: %s (%d bytes)", wID, chunkID, written)
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte("Chunk saved"))

		case http.MethodDelete:
			// Delete chunk file
			err := os.Remove(filePath)
			if err != nil {
				if os.IsNotExist(err) {
					http.Error(w, "Chunk not found", http.StatusNotFound)
				} else {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return
			}
			log.Printf("[%s] Deleted chunk: %s", wID, chunkID)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Chunk deleted"))

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func startHeartbeatLoop(id, urlStr, masterURL string) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	// Define heartbeat function
	sendHeartbeat := func() {
		total, free, used := getStorageStats()
		req := common.HeartbeatRequest{
			ID:         id,
			URL:        urlStr,
			TotalSpace: total,
			FreeSpace:  free,
			UsedSpace:  used,
		}

		payload, err := json.Marshal(req)
		if err != nil {
			log.Printf("Error marshalling heartbeat: %v", err)
			return
		}

		heartbeatURL := fmt.Sprintf("%s/api/heartbeat", strings.TrimSuffix(masterURL, "/"))
		resp, err := http.Post(heartbeatURL, "application/json", strings.NewReader(string(payload)))
		if err != nil {
			log.Printf("Heartbeat failed (Master offline?): %v", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("Heartbeat rejected by master: status=%d, response=%s", resp.StatusCode, string(body))
		}
	}

	// Trigger immediately on start
	sendHeartbeat()

	for range ticker.C {
		sendHeartbeat()
	}
}
