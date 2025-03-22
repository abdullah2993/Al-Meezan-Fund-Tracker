package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

const (
	maxFileSize = 512 * 1024 // 512 KB limit
)

// Config holds the application configuration
type Config struct {
	APIKey        string
	Port          string
	DatabasePath  string
}

// Fund represents a structured fund entry
type Fund struct {
	Name           string     `json:"name"`
	LaunchDate     *time.Time `json:"launch_date,omitempty"`
	ValidityDate   *time.Time `json:"validity_date,omitempty"`
	Repurchase     *float64   `json:"repurchase,omitempty"`
	Offer          *float64   `json:"offer,omitempty"`
	NAV            *float64   `json:"nav,omitempty"`
	MTD            *float64   `json:"mtd,omitempty"`
	FYTD           *float64   `json:"fytd,omitempty"`
	CYTD           *float64   `json:"cytd,omitempty"`
	FY24           *float64   `json:"fy24,omitempty"`
	FY23           *float64   `json:"fy23,omitempty"`
	SinceInception *float64   `json:"since_inception,omitempty"`
	UploadDate     time.Time  `json:"upload_date"`
}

// Server represents the HTTP server with its dependencies
type Server struct {
	config Config
	logger *slog.Logger
	db     *sql.DB
}

// NewServer creates a new server instance
func NewServer(config Config, logger *slog.Logger) (*Server, error) {
	// Open SQLite database connection
	db, err := sql.Open("sqlite3", config.DatabasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Set connection parameters
	db.SetMaxOpenConns(1)                  // SQLite only supports one writer at a time
	db.SetMaxIdleConns(1)                  // Keep connection open
	db.SetConnMaxLifetime(time.Hour * 24)  // Reasonable lifetime

	// Create a server instance
	server := &Server{
		config: config,
		logger: logger,
		db:     db,
	}

	// Initialize database schema
	if err := server.initDatabase(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return server, nil
}

// initDatabase creates the necessary tables if they don't exist
func (s *Server) initDatabase() error {
	// Create funds table
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS funds (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		launch_date TEXT,
		validity_date TEXT,
		repurchase REAL,
		offer REAL,
		nav REAL,
		mtd REAL,
		fytd REAL,
		cytd REAL,
		fy24 REAL,
		fy23 REAL,
		since_inception REAL,
		upload_date TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_fund_name ON funds(name);
	CREATE INDEX IF NOT EXISTS idx_upload_date ON funds(upload_date);
	`

	_, err := s.db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}
	return nil
}

// Close closes the database connection
func (s *Server) Close() error {
	return s.db.Close()
}

// parseFloat safely parses a string to a float, returning nil if invalid
func parseFloat(value string) *float64 {
	// Remove any trailing characters and whitespace
	value = strings.TrimSpace(value)
	value = strings.TrimRight(value, "*%")

	if value == "" {
		return nil
	}

	num, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil
	}

	return &num
}

// parseDate attempts to parse a date string in multiple formats
func parseDate(date string) *time.Time {
	date = strings.TrimSpace(date)
	if date == "" {
		return nil
	}

	formats := []string{
		"Jan 2, 2006",
		"2 Jan, 2006",
		"January 2, 2006",
		"2 January, 2006",
	}

	for _, format := range formats {
		t, err := time.Parse(format, date)
		if err == nil {
			return &t
		}
	}

	return nil
}

// parseHTML extracts fund information from HTML content
func parseHTML(ctx context.Context, logger *slog.Logger, htmlContent string, uploadDate time.Time) ([]Fund, error) {
	var funds []Fund

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		logger.ErrorContext(ctx, "failed to parse HTML document", "error", err)
		return nil, fmt.Errorf("failed to parse HTML document: %w", err)
	}

	logger.InfoContext(ctx, "starting HTML parsing")

	// Iterate over each row in the table
	doc.Find("tr[align='center']").Each(func(index int, row *goquery.Selection) {
		columns := row.Find("td")

		// Skip rows with less than required columns
		if columns.Length() < 12 {
			return
		}

		cleanText := func(idx int) string {
			return strings.TrimSpace(columns.Eq(idx).Text())
		}

		fund := Fund{
			Name:           strings.TrimRight(cleanText(0), "*"),
			LaunchDate:     parseDate(cleanText(1)),
			ValidityDate:   parseDate(cleanText(2)),
			Repurchase:     parseFloat(cleanText(3)),
			Offer:          parseFloat(cleanText(4)),
			NAV:            parseFloat(cleanText(5)),
			MTD:            parseFloat(cleanText(6)),
			FYTD:           parseFloat(cleanText(7)),
			CYTD:           parseFloat(cleanText(8)),
			FY24:           parseFloat(cleanText(9)),
			FY23:           parseFloat(cleanText(10)),
			SinceInception: parseFloat(cleanText(11)),
			UploadDate:     uploadDate,
		}

		funds = append(funds, fund)
	})

	logger.InfoContext(ctx, "completed HTML parsing", "fund_count", len(funds))

	return funds, nil
}

// storeFunds saves fund data to the SQLite database
func (s *Server) storeFunds(ctx context.Context, funds []Fund) error {
	// Begin a transaction
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback if not committed

	// Prepare the insert statement
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO funds (
			name, launch_date, validity_date, repurchase, offer, nav, 
			mtd, fytd, cytd, fy24, fy23, since_inception, upload_date
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Helper function to format date pointers
	formatDate := func(t *time.Time) interface{} {
		if t == nil {
			return nil
		}
		return t.Format(time.RFC3339)
	}

	// Insert each fund
	for _, fund := range funds {
		_, err := stmt.ExecContext(ctx,
			fund.Name,
			formatDate(fund.LaunchDate),
			formatDate(fund.ValidityDate),
			fund.Repurchase,
			fund.Offer,
			fund.NAV,
			fund.MTD,
			fund.FYTD,
			fund.CYTD,
			fund.FY24,
			fund.FY23,
			fund.SinceInception,
			fund.UploadDate.Format(time.RFC3339),
		)
		if err != nil {
			return fmt.Errorf("failed to insert fund '%s': %w", fund.Name, err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// uploadHandler processes the HTML file upload and returns structured data
func (s *Server) uploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Create a request-specific logger with request info
	requestID := fmt.Sprintf("%d", time.Now().UnixNano())
	logger := s.logger.With(
		"request_id", requestID,
		"remote_addr", r.RemoteAddr,
		"method", r.Method,
		"path", r.URL.Path,
	)

	// Validate API Key
	if r.Header.Get("X-API-Key") != s.config.APIKey {
		logger.WarnContext(ctx, "unauthorized access attempt")
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Enforce POST method
	if r.Method != http.MethodPost {
		logger.WarnContext(ctx, "method not allowed", "method", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	logger.InfoContext(ctx, "processing upload request")

	// Parse multipart form data (limit total file size)
	if err := r.ParseMultipartForm(maxFileSize); err != nil {
		logger.ErrorContext(ctx, "failed to parse multipart form", "error", err)
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Get file from form data
	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		logger.ErrorContext(ctx, "invalid file upload", "error", err)
		http.Error(w, "Invalid file upload", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Get the date parameter from the form, default to current time if not provided
	uploadDate := time.Now()
	if dateStr := r.FormValue("date"); dateStr != "" {
		parsedDate, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			logger.WarnContext(ctx, "invalid date format", "date", dateStr, "error", err)
			http.Error(w, "Invalid date format. Please use YYYY-MM-DD", http.StatusBadRequest)
			return
		}
		uploadDate = parsedDate
	}

	logger = logger.With(
		"filename", fileHeader.Filename, 
		"filesize", fileHeader.Size,
		"upload_date", uploadDate.Format("2006-01-02"),
	)
	logger.InfoContext(ctx, "file received")

	// Verify file size is within limits
	if fileHeader.Size > maxFileSize {
		logger.WarnContext(ctx, "file size exceeds maximum allowed size",
			"max_size", maxFileSize,
			"actual_size", fileHeader.Size)
		http.Error(w, "File too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Read file contents with size limit as additional protection
	limitedReader := io.LimitReader(file, maxFileSize)
	fileBytes, err := io.ReadAll(limitedReader)
	if err != nil {
		logger.ErrorContext(ctx, "failed to read file", "error", err)
		http.Error(w, "Failed to read file", http.StatusInternalServerError)
		return
	}

	// Decode HTML entities
	htmlContent := html.UnescapeString(string(fileBytes))

	// Parse HTML to extract fund details
	funds, err := parseHTML(ctx, logger, htmlContent, uploadDate)
	if err != nil {
		logger.ErrorContext(ctx, "failed to parse HTML content", "error", err)
		http.Error(w, "Failed to parse HTML: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if len(funds) == 0 {
		logger.WarnContext(ctx, "no funds found in the provided HTML")
		http.Error(w, "No funds found in the provided HTML", http.StatusBadRequest)
		return
	}

	// Store the funds in the database
	if err := s.storeFunds(ctx, funds); err != nil {
		logger.ErrorContext(ctx, "failed to store funds in database", "error", err)
		http.Error(w, "Failed to store funds in database: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Encode response as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(funds); err != nil {
		logger.ErrorContext(ctx, "failed to encode JSON response", "error", err)
		http.Error(w, "Failed to generate response", http.StatusInternalServerError)
		return
	}

	logger.InfoContext(ctx, "request completed successfully", 
		"fund_count", len(funds),
		"stored_in_db", true,
	)
}

// healthHandler provides a basic health check endpoint
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Test database connection
	if err := s.db.PingContext(ctx); err != nil {
		s.logger.ErrorContext(ctx, "database health check failed", "error", err)
		http.Error(w, "Database connection failed", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "OK")
}

func main() {
	// Configure structured logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Get configuration from environment with fallbacks
	config := Config{
		APIKey:       getEnv("API_KEY", "your-secret-api-key"),
		Port:         getEnv("PORT", "8089"),
		DatabasePath: getEnv("DB_PATH", "./funds.db"),
	}

	// Validate config
	if config.APIKey == "your-secret-api-key" {
		logger.Warn("using default API key, consider setting API_KEY environment variable")
	}

	// Create server
	server, err := NewServer(config, logger)
	if err != nil {
		logger.Error("failed to initialize server", "error", err)
		os.Exit(1)
	}
	defer server.Close()

	// Register handlers
	http.HandleFunc("/upload", server.uploadHandler)
	http.HandleFunc("/health", server.healthHandler)

	// Start server
	addr := ":" + config.Port
	logger.Info("server starting", 
		"port", config.Port,
		"database", config.DatabasePath,
	)

	// Use server with timeout handling
	srv := &http.Server{
		Addr:         addr,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
}

// getEnv gets an environment variable or returns the fallback value
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}