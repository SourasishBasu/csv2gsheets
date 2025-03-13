package main

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

const (
	// Configuration constants
	maxChunkCells               = 282400 //Maximum cells per write operation to Google Sheets
	port                        = ":8080"
	credentialsFile             = "credentials.json" // Your Google API credentials
	tempDir                     = "temp"
	maxConcurrentUploads        = 5        // Limit concurrent uploads to avoid rate limiting
	maxCellsPerSheet            = 10000000 // Google Sheets has a limit on 10 million cells
	switchSpreadsheetChunkCount = 35
)

type ProcessStats struct {
	DecompressionTime time.Duration
	CSVParsingTime    time.Duration
	SheetCreationTime time.Duration
	UploadTime        time.Duration
	TotalTime         time.Duration
	RowsProcessed     int
	ChunksUploaded    int
}

func main() {
	// Create temp directory if it doesn't exist
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		os.Mkdir(tempDir, 0755)
	}

	// Set up more verbose logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("Starting server on port %s", port)
	log.Printf("System Info: %s with %d CPUs", runtime.GOOS, runtime.NumCPU())
	log.Printf("Go Version: %s", runtime.Version())

	http.HandleFunc("/upload", handleUpload)
	log.Printf("Server started and listening on port %s", port)
	log.Fatal(http.ListenAndServe(port, nil))
}

func makeSpreadsheetPublic(driveService *drive.Service, spreadsheetID string) error {
	permission := &drive.Permission{
		Type: "anyone", // Anyone can access
		Role: "reader", // Read-only access
	}

	_, err := driveService.Permissions.Create(spreadsheetID, permission).Do()
	if err != nil {
		return fmt.Errorf("failed to make spreadsheet public: %v", err)
	}

	return nil
}

// Create multiple spreadsheets beforehand and return their IDs
func createSpreadsheets(sheetsService *sheets.Service, driveService *drive.Service, rows int, cols int, numSheets int) ([]string, error) {
	var spreadsheetIDs []string
	for i := 1; i <= numSheets; i++ {
		title := fmt.Sprintf("CSV Import %s - %d", time.Now().Format("2006-01-02 15:04:05"), i)

		// Define the spreadsheet with a single sheet and fixed column count
		spreadsheet, err := sheetsService.Spreadsheets.Create(&sheets.Spreadsheet{
			Properties: &sheets.SpreadsheetProperties{Title: title},
			Sheets: []*sheets.Sheet{
				{
					Properties: &sheets.SheetProperties{
						Title: "Sheet1",
						GridProperties: &sheets.GridProperties{
							RowCount:    int64(rows), // Max rows
							ColumnCount: int64(cols), // Restrict to 8 columns
						},
					},
				},
			},
		}).Do()
		if err != nil {
			log.Printf("Error creating spreadsheet %d: %v\n", i, err)
			return nil, err
		}

		if err := makeSpreadsheetPublic(driveService, spreadsheet.SpreadsheetId); err != nil {
			log.Printf("Error making spreadsheet public: %v", err)
		}

		spreadsheetIDs = append(spreadsheetIDs, spreadsheet.SpreadsheetId)
		log.Printf("Created spreadsheet: %s (ID: %s)", title, spreadsheet.SpreadsheetId)
	}

	return spreadsheetIDs, nil
}

/*
func getSheetMetadata(sheetsService *sheets.Service, spreadsheetID string) error {
	resp, err := sheetsService.Spreadsheets.Get(spreadsheetID).Do()
	if err != nil {
		return fmt.Errorf("error fetching spreadsheet metadata: %v", err)
	}

	for _, sheet := range resp.Sheets {
		fmt.Printf("Sheet: %s | Rows: %d | Columns: %d\n",
			sheet.Properties.Title, sheet.Properties.GridProperties.RowCount, sheet.Properties.GridProperties.ColumnCount)
	}
	return nil
}
*/

func handleUpload(w http.ResponseWriter, r *http.Request) {
	// Enable CORS headers for frontend communication
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	// Handle preflight OPTIONS request
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Start total time measurement
	totalStart := time.Now()
	stats := ProcessStats{}
	requestID := fmt.Sprintf("req-%d", time.Now().UnixNano())

	log.Printf("[%s] New upload request received", requestID)

	// Only accept POST requests
	if r.Method != http.MethodPost {
		log.Printf("[%s] Method not allowed: %s", requestID, r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Limit request size to avoid memory issues (100MB)
	r.Body = http.MaxBytesReader(w, r.Body, 100*1024*1024)

	// Check Content-Type
	contentType := r.Header.Get("Content-Type")
	log.Printf("[%s] Content-Type: %s", requestID, contentType)

	// Get the file from the request
	log.Printf("[%s] Parsing multipart form data", requestID)
	err := r.ParseMultipartForm(10 << 20) // 10MB buffer
	if err != nil {
		log.Printf("[%s] Error parsing multipart form: %v", requestID, err)
		http.Error(w, "Error parsing form: "+err.Error(), http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		log.Printf("[%s] Error retrieving file: %v", requestID, err)
		http.Error(w, "Error retrieving file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	log.Printf("[%s] Received file: %s, size: %d bytes", requestID, header.Filename, header.Size)

	// Create temporary file to store the uploaded file
	tempFile, err := os.CreateTemp(tempDir, "upload-*.gz")
	if err != nil {
		log.Printf("[%s] Error creating temp file: %v", requestID, err)
		http.Error(w, "Error creating temp file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Copy the uploaded file to the temporary file
	log.Printf("[%s] Saving uploaded file to temp location: %s", requestID, tempFile.Name())
	bytesWritten, err := io.Copy(tempFile, file)
	if err != nil {
		log.Printf("[%s] Error saving file: %v", requestID, err)
		http.Error(w, "Error saving file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("[%s] Saved %d bytes to temp file", requestID, bytesWritten)

	tempFile.Close() // Close to ensure all data is written

	// Reopen the file for reading
	log.Printf("[%s] Reopening temp file for processing", requestID)
	tempFile, err = os.Open(tempFile.Name())
	if err != nil {
		log.Printf("[%s] Error reopening temp file: %v", requestID, err)
		http.Error(w, "Error reopening temp file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Start decompression timing
	log.Printf("[%s] Starting decompression", requestID)
	decompStart := time.Now()

	// Decompress the gzip file
	gzipReader, err := gzip.NewReader(tempFile)
	if err != nil {
		log.Printf("[%s] Error decompressing file: %v", requestID, err)
		http.Error(w, "Error decompressing file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer gzipReader.Close()

	// End decompression timing
	stats.DecompressionTime = time.Since(decompStart)
	log.Printf("[%s] Decompression completed in %v", requestID, stats.DecompressionTime)

	// Start CSV parsing timing
	log.Printf("[%s] Starting CSV parsing", requestID)
	parseStart := time.Now()

	// Parse the CSV
	csvReader := csv.NewReader(gzipReader)
	// Increase buffer for large CSV files
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields

	// Read header row
	log.Printf("[%s] Reading CSV header", requestID)
	var csvheader []string
	csvheader, err = csvReader.Read()
	if err != nil {
		log.Printf("[%s] Error reading CSV header: %v", requestID, err)
		http.Error(w, "Error reading CSV header: "+err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("[%s] CSV header has %d columns", requestID, len(csvheader))

	// Read all rows in batches to avoid memory issues with large files
	log.Printf("[%s] Reading CSV rows", requestID)
	var allRows [][]string
	allRows = append(allRows, csvheader) // Add header to rows

	rowCount := 1 // Start at 1 for header

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("[%s] Error parsing CSV at row %d: %v", requestID, rowCount, err)
			http.Error(w, fmt.Sprintf("Error parsing CSV at row %d: %v", rowCount, err), http.StatusBadRequest)
			return
		}

		allRows = append(allRows, row)
		rowCount++

	}

	stats.RowsProcessed = len(allRows) - 1 // Exclude header from count
	log.Printf("[%s] Finished reading CSV: %d rows processed", requestID, stats.RowsProcessed)

	// End CSV parsing timing
	stats.CSVParsingTime = time.Since(parseStart)
	log.Printf("[%s] CSV parsing completed in %v", requestID, stats.CSVParsingTime)

	numSheets := int(math.Ceil(float64(len(allRows)*len(csvheader)) / maxCellsPerSheet))
	log.Printf("[%s] Spreadsheets Required: %d", requestID, numSheets)

	// Initialize Google Sheets API
	log.Printf("[%s] Initializing Google Sheets API", requestID)
	sheetsService, driveService, err := initSheetsService()
	if err != nil {
		log.Printf("[%s] Error initializing Sheets API: %v", requestID, err)
		http.Error(w, "Error initializing Sheets API: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Start sheet creation timing
	log.Printf("[%s] Creating Google Spreadsheets", requestID)
	sheetStart := time.Now()

	// Create the required number of spreadsheets
	spreadsheetIDs, err := createSpreadsheets(sheetsService, driveService, len(allRows), len(csvheader), numSheets)
	if err != nil {
		http.Error(w, "Error creating spreadsheets", http.StatusInternalServerError)
		return
	}

	// End sheet creation timing
	stats.SheetCreationTime = time.Since(sheetStart)
	log.Printf("[%s] Spreadsheet creation completed in %v", requestID, stats.SheetCreationTime)

	// Start upload timing
	log.Printf("[%s] Starting data upload to Google Sheets", requestID)
	uploadStart := time.Now()

	// Process data in chunks
	maxChunkSize := int(math.Floor(maxChunkCells / float64(len(csvheader))))
	numChunks := int(math.Ceil(float64(len(allRows)) / float64(maxChunkSize)))
	//numChunks := (len(allRows) + maxChunkSize - 1) / maxChunkSize
	currentSpreadsheet := 0
	stats.ChunksUploaded = numChunks
	log.Printf("[%s] Uploading data in %d chunks (max %d rows per chunk)",
		requestID, numChunks, maxChunkSize)

	// Use a semaphore to limit concurrent uploads
	semaphore := make(chan struct{}, maxConcurrentUploads)

	// Use a WaitGroup to wait for all goroutines to complete
	var wg sync.WaitGroup
	errorChan := make(chan error, numChunks)
	progressChan := make(chan int, numChunks)

	// Start a goroutine to track progress
	go func() {
		completedChunks := 0
		for range progressChan {
			completedChunks++
			if completedChunks%5 == 0 || completedChunks == numChunks {
				log.Printf("[%s] Upload progress: %d/%d chunks (%.1f%%)",
					requestID, completedChunks, numChunks, float64(completedChunks)/float64(numChunks)*100)
			}
		}
	}()

	for i := 0; i < numChunks; i++ {
		wg.Add(1)

		// Acquire semaphore
		semaphore <- struct{}{}

		if i > 0 && i%switchSpreadsheetChunkCount == 0 && currentSpreadsheet < len(spreadsheetIDs)-1 {
			currentSpreadsheet++
		}

		spreadsheetID := spreadsheetIDs[currentSpreadsheet]

		go func(chunkIndex int, spreadsheetID string) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release semaphore

			start := chunkIndex * maxChunkSize
			end := (chunkIndex + 1) * maxChunkSize
			if end > len(allRows) {
				end = len(allRows)
			}
			chunk := allRows[start:end]

			// Convert the chunk to ValueRange
			valueRange := &sheets.ValueRange{
				Values: make([][]interface{}, len(chunk)),
			}

			for i, row := range chunk {
				valueRange.Values[i] = make([]interface{}, len(row))
				for j, cell := range row {
					valueRange.Values[i][j] = cell
				}
			}

			// Calculate the range for this chunk
			startRow := start + 1 // 1-indexed
			endRow := end
			//rangeStr := fmt.Sprintf("A%d:%c%d", startRow, 'A'+len(csvheader)-1, endRow)
			rangeStr := fmt.Sprintf("Sheet1!A%d:H%d", startRow, endRow)

			log.Printf("[%s] Chunk %d: Uploading range %s to spreadsheet %s",
				requestID, chunkIndex, rangeStr, spreadsheetID)

			// Add retries for API calls
			maxRetries := 3
			var updateErr error

			for retry := 0; retry < maxRetries; retry++ {
				// Update the spreadsheet with this chunk
				_, updateErr = sheetsService.Spreadsheets.Values.Update(
					spreadsheetID,
					rangeStr,
					valueRange,
				).ValueInputOption("RAW").Do()

				if updateErr == nil {
					/*
						err := getSheetMetadata(sheetsService, spreadsheetID)
						if err != nil {
							log.Printf("[%s] Error fetching sheet metadata: %v", requestID, err)
						}
					*/

					break // Success, exit retry loop
				}

				log.Printf("[%s] Error updating chunk %d (retry %d/3) in spreadsheet %d: %v",
					requestID, chunkIndex, retry+1, currentSpreadsheet+1, updateErr)

				// Wait before retrying (exponential backoff)
				if retry < maxRetries-1 {
					time.Sleep(time.Duration(1<<retry) * time.Second)
				}
			}

			if updateErr != nil {
				errorChan <- fmt.Errorf("error updating chunk %d in sheet %d after %d retries: %v",
					chunkIndex, currentSpreadsheet+1, maxRetries, updateErr)
				return
			}

			progressChan <- 1 // Report progress
		}(i, spreadsheetID)
	}

	// Wait for all chunks to be processed
	wg.Wait()
	close(errorChan)
	close(progressChan)

	// Check for errors
	var uploadErrors []error
	for err := range errorChan {
		uploadErrors = append(uploadErrors, err)
	}

	if len(uploadErrors) > 0 {
		log.Printf("[%s] Encountered %d errors during upload", requestID, len(uploadErrors))
		for i, err := range uploadErrors {
			if i < 5 { // Log first 5 errors only
				log.Printf("[%s] Upload error %d: %v", requestID, i+1, err)
			}
		}
		http.Error(w, fmt.Sprintf("Encountered %d errors during upload. First error: %v",
			len(uploadErrors), uploadErrors[0]), http.StatusInternalServerError)
		return
	}

	// End upload timing
	stats.UploadTime = time.Since(uploadStart)
	log.Printf("[%s] Data upload completed in %v", requestID, stats.UploadTime)

	// Calculate total time
	stats.TotalTime = time.Since(totalStart)
	log.Printf("[%s] Total processing time: %v", requestID, stats.TotalTime)

	// Return success response with timing statistics and spreadsheet link
	log.Printf("[%s] Sending success response to client", requestID)

	spreadsheetURLs := ""
	for i, spreadsheetID := range spreadsheetIDs {
		spreadsheetURLs += fmt.Sprintf("Spreadsheet %d: https://docs.google.com/spreadsheets/d/%s\n", i+1, spreadsheetID)
	}

	response := fmt.Sprintf(`
Processing completed successfully:
- Total time: %v
- Decompression time: %v
- CSV parsing time: %v
- Sheet creation time: %v
- Upload time: %v
- Rows processed: %d
- Chunks uploaded: %d
- Spreadsheet URL: %s

Note: Google Sheets has a limit of approximately 5 million cells. If your CSV is very large, 
only a portion may be visible in the spreadsheet.
`,
		stats.TotalTime,
		stats.DecompressionTime,
		stats.CSVParsingTime,
		stats.SheetCreationTime,
		stats.UploadTime,
		stats.RowsProcessed,
		stats.ChunksUploaded,
		spreadsheetURLs,
	)

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(response))

	log.Printf("[%s] Response: %s", requestID, response)
	log.Printf("[%s] Request completed successfully", requestID)
}

// Initialize Google Sheets API service
func initSheetsService() (*sheets.Service, *drive.Service, error) {
	ctx := context.Background()

	// Read credentials file
	credentialsBytes, err := os.ReadFile(credentialsFile)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to read client credentials file: %v", err)
	}

	// Configure the Google Sheets API client
	config, err := google.JWTConfigFromJSON(credentialsBytes, sheets.SpreadsheetsScope, drive.DriveFileScope)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to parse client credentials file: %v", err)
	}

	client := config.Client(ctx)

	// Create the Sheets service
	srv, err := sheets.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create Sheets service: %v", err)
	}

	// Create the Drive service (for sharing permissions)
	driveService, err := drive.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create Drive service: %v", err)
	}

	return srv, driveService, nil
}
