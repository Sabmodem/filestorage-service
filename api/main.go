package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	_ "filestorage-api/docs"

	echoSwagger "github.com/swaggo/echo-swagger"
)

var (
	s3Region            = os.Getenv("S3_REGION")               // AWS Region, e.g., "us-east-1"
	s3Bucket            = os.Getenv("S3_BUCKET")               // S3 Bucket name
	listenPort          = os.Getenv("PORT")                    // Port to listen on, defaults to "8080"
	awsEndpoint         = os.Getenv("AWS_ENDPOINT")            // MinIO endpoint, e.g., "http://minio:9000"
	awsS3ForcePathStyle = os.Getenv("AWS_S3_FORCE_PATH_STYLE") // "true" for MinIO
)

var (
	s3Client   *s3.S3
	s3Uploader *s3manager.Uploader
)

const (
	maxFileSizeMB         = 50
	maxFileSizeByte int64 = maxFileSizeMB * 1024 * 1024 // 50 MB in bytes
)

type FileInfo struct {
	Filename   string    `json:"filename"`
	Path       string    `json:"path"`
	UploadedAt time.Time `json:"uploaded_at"`
}

// initS3Client initializes the AWS S3 session and client.
// It uses environment variables for configuration.
func initS3Client() {
	if s3Region == "" || s3Bucket == "" {
		log.Fatal("S3_REGION and S3_BUCKET environment variables must be set.")
	}

	cfg := &aws.Config{
		Region: aws.String(s3Region),
	}

	if awsEndpoint != "" {
		cfg.Endpoint = aws.String(awsEndpoint)
		log.Printf("Using S3 Endpoint: %s", *cfg.Endpoint)
	}

	if strings.ToLower(awsS3ForcePathStyle) == "true" {
		cfg.S3ForcePathStyle = aws.Bool(true)
		log.Println("S3 Force Path Style enabled.")
	}

	sess, err := session.NewSession(cfg)
	if err != nil {
		log.Fatalf("Failed to create AWS session: %v", err)
	}

	s3Client = s3.New(sess)
	s3Uploader = s3manager.NewUploader(sess)
	log.Println("S3 client initialized successfully.")
}

// @title File Storage Service API
// @version 1.0
// @description A simple microservice for storing and retrieving files, with S3/MinIO backend.
// @BasePath /
func main() {
	if listenPort == "" {
		listenPort = "8080"
	}

	initS3Client()
	e := echo.New()
	e.Use(middleware.Logger())  // Request logging
	e.Use(middleware.Recover()) // Recover from panics
	e.Use(middleware.CORS())    // Enable CORS (adjust as needed for production)
	e.GET("/", rootHandler)
	e.GET("/health", healthCheckHandler)
	// Swagger UI route
	// The URL for Swagger UI will be http://localhost:8080/swagger/index.html
	e.GET("/swagger/*", echoSwagger.WrapHandler)
	e.GET("/files", listFilesHandler)
	e.POST("/files", uploadFilesHandler)
	e.GET("/files/:filename", getFileHandler)
	e.DELETE("/files/:filename", deleteFileHandler)
	log.Printf("Starting Go File Storage Service on :%s", listenPort)
	if err := e.Start(":" + listenPort); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed to start: %v", err)
	}
}

// rootHandler provides a welcome message.
// @Summary Welcome message
// @Description Provides a welcome message for the service.
// @Tags General
// @Produce json
// @Success 200 {object} map[string]string "message: Welcome to the File Storage Service. Visit /swagger/index.html for API documentation."
// @Router / [get]
func rootHandler(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"message": "Welcome to the File Storage Service. Visit /swagger/index.html for API documentation."})
}

// healthCheckHandler provides a simple health check endpoint.
// @Summary Health check
// @Description Checks the health status of the service.
// @Tags General
// @Produce json
// @Success 200 {object} map[string]string "status: healthy"
// @Router /health [get]
func healthCheckHandler(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"status": "healthy"})
}

// listFilesHandler lists all objects in the configured S3 bucket.
// @Summary List all files
// @Description Lists all available files in the S3 bucket. Assumes authentication/authorization by gateway.
// @Tags Files
// @Produce json
// @Success 200 {array} FileInfo "List of files"
// @Failure 500 {object} map[string]string "detail: Failed to list files from storage."
// @Router /files [get]
func listFilesHandler(c echo.Context) error {
	userPreferredUsername := c.Request().Header.Get("X-User-Preferred-Username")
	if userPreferredUsername == "" {
		userPreferredUsername = "N/A (no X-User-Preferred-Username header)"
	}

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Bucket),
	}

	result, err := s3Client.ListObjectsV2(input)
	if err != nil {
		log.Printf("Error listing S3 objects for user %s: %v", userPreferredUsername, err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list files from storage.")
	}

	var files []FileInfo
	for _, obj := range result.Contents {
		if obj.Key != nil && !strings.HasSuffix(*obj.Key, "/") {
			files = append(files, FileInfo{
				Filename:   *obj.Key,
				Path:       fmt.Sprintf("/files/%s", *obj.Key),
				UploadedAt: *obj.LastModified,
			})
		}
	}
	log.Printf("Files listed successfully for user: %s", userPreferredUsername)
	return c.JSON(http.StatusOK, files)
}

// uploadFilesHandler handles uploading one or more files to S3.
// @Summary Upload files
// @Description Uploads one or more files to the service. Assumes authentication/authorization by gateway.
// @Tags Files
// @Accept multipart/form-data
// @Param files formData file true "Files to upload" collectionFormat multi
// @Success 201 {object} map[string]interface{} "message: Files uploaded successfully, uploaded_files: [filename1, filename2]"
// @Failure 400 {object} map[string]string "detail: No files provided for upload."
// @Failure 413 {object} map[string]string "detail: File exceeds the maximum allowed size."
// @Failure 500 {object} map[string]string "detail: Could not upload file."
// @Router /files [post]
func uploadFilesHandler(c echo.Context) error {
	userPreferredUsername := c.Request().Header.Get("X-User-Preferred-Username")
	if userPreferredUsername == "" {
		userPreferredUsername = "N/A (no X-User-Preferred-Username header)"
	}

	form, err := c.MultipartForm()
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Failed to parse multipart form: %v", err))
	}

	files := form.File["files"]
	if len(files) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "No files provided for upload.")
	}

	var uploadedFilenames []string
	for _, fileHeader := range files {
		if fileHeader.Filename == "" {
			log.Println("Received an uploaded file without a filename.")
			continue
		}

		file, err := fileHeader.Open()
		if err != nil {
			log.Printf("Error opening uploaded file '%s': %v", fileHeader.Filename, err)
			return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Could not open file '%s'", fileHeader.Filename))
		}
		defer file.Close()

		uniqueFilename := fmt.Sprintf("%s_%s", uuid.New().String(), filepath.Base(fileHeader.Filename))

		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			var currentSize int64
			buf := make([]byte, 32*1024)
			for {
				n, readErr := file.Read(buf)
				if n > 0 {
					currentSize += int64(n)
					if currentSize > maxFileSizeByte {
						pw.CloseWithError(fmt.Errorf("file '%s' exceeds the maximum allowed size of %dMB", fileHeader.Filename, maxFileSizeMB))
						return
					}
					if _, writeErr := pw.Write(buf[:n]); writeErr != nil {
						pw.CloseWithError(writeErr)
						return
					}
				}
				if readErr == io.EOF {
					break
				}
				if readErr != nil {
					pw.CloseWithError(readErr)
					return
				}
			}
		}()

		uploadInput := &s3manager.UploadInput{
			Bucket: aws.String(s3Bucket),
			Key:    aws.String(uniqueFilename),
			Body:   pr,
		}

		_, err = s3Uploader.Upload(uploadInput)
		if err != nil {
			if strings.Contains(err.Error(), fmt.Sprintf("exceeds the maximum allowed size of %dMB", maxFileSizeMB)) {
				return echo.NewHTTPError(http.StatusRequestEntityTooLarge, err.Error())
			}
			log.Printf("Failed to upload file '%s' to S3 as '%s' for user %s: %v", fileHeader.Filename, uniqueFilename, userPreferredUsername, err)
			return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Could not upload file '%s'", fileHeader.Filename))
		}
		uploadedFilenames = append(uploadedFilenames, uniqueFilename)
		log.Printf("File '%s' uploaded successfully as '%s' by user: %s", fileHeader.Filename, uniqueFilename, userPreferredUsername)
	}
	return c.JSON(http.StatusCreated, map[string]interface{}{"message": "Files uploaded successfully", "uploaded_files": uploadedFilenames})
}

// getFileHandler retrieves a specific file from S3 and streams it to the client.
// @Summary Get a file
// @Description Retrieves a specific file by its filename. Assumes authentication/authorization by gateway.
// @Tags Files
// @Produce octet-stream
// @Param filename path string true "Name of the file to retrieve"
// @Success 200 {file} byte "File content"
// @Failure 404 {object} map[string]string "detail: File not found."
// @Failure 500 {object} map[string]string "detail: Failed to retrieve file."
// @Router /files/{filename} [get]
func getFileHandler(c echo.Context) error {
	userPreferredUsername := c.Request().Header.Get("X-User-Preferred-Username")
	if userPreferredUsername == "" {
		userPreferredUsername = "N/A (no X-User-Preferred-Username header)"
	}

	filename := c.Param("filename")

	input := &s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(filename),
	}

	result, err := s3Client.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			log.Printf("Attempted to access non-existent file: %s by user: %s", filename, userPreferredUsername)
			return echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("File '%s' not found.", filename))
		}
		log.Printf("Error getting object '%s' from S3 for user %s: %v", filename, userPreferredUsername, err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to retrieve file.")
	}
	defer result.Body.Close()

	c.Response().Header().Set("Content-Type", aws.StringValue(result.ContentType))
	c.Response().Header().Set("Content-Length", fmt.Sprintf("%d", aws.Int64Value(result.ContentLength)))
	c.Response().Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))

	if _, err := io.Copy(c.Response().Writer, result.Body); err != nil {
		log.Printf("Error streaming file '%s' from S3 to client for user %s: %v", filename, userPreferredUsername, err)
	}

	log.Printf("Serving file: %s to user: %s", filename, userPreferredUsername)
	return nil
}

// deleteFileHandler deletes a specific file from S3.
// @Summary Delete a file
// @Description Deletes a specific file by its filename. Assumes authentication/authorization by gateway.
// @Tags Files
// @Param filename path string true "Name of the file to delete"
// @Success 204 "No Content"
// @Failure 500 {object} map[string]string "detail: Could not delete file."
// @Router /files/{filename} [delete]
func deleteFileHandler(c echo.Context) error {
	userPreferredUsername := c.Request().Header.Get("X-User-Preferred-Username")
	if userPreferredUsername == "" {
		userPreferredUsername = "N/A (no X-User-Preferred-Username header)"
	}

	filename := c.Param("filename")

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(filename),
	}

	_, err := s3Client.DeleteObject(input)
	if err != nil {
		log.Printf("Error deleting S3 object '%s' for user %s: %v", filename, userPreferredUsername, err)
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Could not delete file '%s'.", filename))
	}

	log.Printf("File '%s' deleted successfully by user: %s", filename, userPreferredUsername)
	return c.NoContent(http.StatusNoContent)
}
