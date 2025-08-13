package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/joho/godotenv"
)

// Payload represents the incoming JSON structure.
type Payload struct {
	Number int `json:"number"`
}

// ResponsePayload represents the outgoing JSON structure.
type ResponsePayload struct {
	ObjectsWritten int      `json:"objects_written"`
	TimeTaken      string   `json:"time_taken"`
	Errors         []string `json:"errors,omitempty"`
}

// Global variables for the GCS client and bucket name.
var (
	gcsClient  *storage.Client
	bucketName string
)

func main() {

	errDot := godotenv.Load()
	if errDot != nil {
		log.Fatal("Error loading .env file")
	}

	fmt.Println("Starting server and writing to ", os.Getenv("GCS_BUCKET_NAME"))

	// Initialize GCS client and bucket name.
	ctx := context.Background()
	var err error
	gcsClient, err = storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create GCS client: %v", err)
	}
	defer gcsClient.Close()

	// Replace with your actual bucket name.
	// You can also use an environment variable: os.Getenv("GCS_BUCKET_NAME")
	bucketName := os.Getenv("GCS_BUCKET_NAME")
	if bucketName == "" {
		log.Fatal("GCS_BUCKET_NAME environment variable not set")
	}

	// Register the handler with a closure to pass the dependencies.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleRequest(w, r, gcsClient, bucketName)
	})

	// Start the web server.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Starting server on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// handleRequest processes the incoming HTTP POST request.
// It now receives the GCS client and bucket name as parameters.
func handleRequest(w http.ResponseWriter, r *http.Request, gcsClient *storage.Client, bucketName string) {
	// Ensure the request method is POST.
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST requests are accepted", http.StatusMethodNotAllowed)
		return
	}

	// Decode the JSON payload.
	var payload Payload
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON payload: %v", err), http.StatusBadRequest)
		return
	}

	// Validate the number.
	if payload.Number <= 0 {
		http.Error(w, "The 'number' value must be a positive integer", http.StatusBadRequest)
		return
	}

	log.Printf("Received request to create %d objects in bucket '%s'", payload.Number, bucketName)

	// Get a reference to the GCS bucket.
	bucket := gcsClient.Bucket(bucketName)

	// Use a context with a timeout for GCS operations.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	// Start timing the GCS write operations.
	startTime := time.Now()

	var wg sync.WaitGroup
	errs := make(chan error, payload.Number)

	for i := 0; i < payload.Number; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Generate a unique object key (timestamp as folder, random hash as name).
			objectKey := generateObjectKey()

			// Generate a random string of 1024 characters for the payload.
			randomString := generateRandomString(1024)

			// Create a new object writer.
			obj := bucket.Object(objectKey).NewWriter(ctx)

			// Write the random string to the object.
			if _, err := io.WriteString(obj, randomString); err != nil {
				errs <- fmt.Errorf("failed to write to GCS object %s: %v", objectKey, err)
				obj.Close() // Best effort close
				return
			}

			// Close the writer to finalize the upload.
			if err := obj.Close(); err != nil {
				errs <- fmt.Errorf("failed to close GCS object writer for %s: %v", objectKey, err)
				return
			}
			log.Printf("Successfully created object: %s", objectKey)
		}()
	}

	wg.Wait()
	close(errs)

	// Calculate the time taken for the loop.
	timeTaken := time.Since(startTime)

	// Collect any errors.
	var errorMessages []string
	for err := range errs {
		errorMessages = append(errorMessages, err.Error())
	}

	// Create the JSON response payload.
	response := ResponsePayload{
		ObjectsWritten: payload.Number - len(errorMessages),
		TimeTaken:      timeTaken.String(),
		Errors:         errorMessages,
	}

	// Set the Content-Type header and encode the response to JSON.
	w.Header().Set("Content-Type", "application/json")
	if len(errorMessages) > 0 {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode JSON response: %v", err)
	}
}

// generateObjectKey creates a unique key using the timestamp as a folder and a random hash as the filename.
func generateObjectKey() string {
	// Get the current time and format it for the folder name (e.g., "20250812T232000").
	timestamp := time.Now().Format("20060102T150405")

	// Generate a 16-byte random hash.
	hashBytes := make([]byte, 16)
	rand.Read(hashBytes)
	randomHash := fmt.Sprintf("%x", hashBytes)

	// Combine the timestamp and hash to create the new key format.
	return fmt.Sprintf("%s/%s", timestamp, randomHash)
}

// generateRandomString creates a random string of the specified length.
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	rand.Read(b)
	for i := 0; i < length; i++ {
		b[i] = charset[b[i]%byte(len(charset))]
	}
	return string(b)
}