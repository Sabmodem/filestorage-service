# File Storage Service

A microservice for storing and retrieving files with S3/MinIO backend, featuring a Vue 3 + Vuetify 3 frontend and OpenResty reverse gateway.

## Quick Start

```bash
docker-compose up --build
```

Access the service at http://localhost

## Architecture

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────┐
│   Browser   │────▶│   OpenResty     │────▶│  Go API     │
│             │     │   (Gateway)     │     │ (S3/MinIO)  │
└─────────────┘     └─────────────────┘     └─────────────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │ Vue 3 + Vuetify │
                   │     Frontend    │
                   └─────────────────┘

                            │
                            ▼
                   ┌─────────────────┐
                   │     MinIO       │
                   │  (Storage)      │
                   └─────────────────┘
```

## Development

### Prerequisites
- Docker Compose
- Go 1.23+ (for local API development)
- Node.js 18+ (for local UI development)

### Running Locally

**API (requires MinIO running):**
```bash
cd api
export S3_REGION=us-west-1
export S3_BUCKET=mytestbucket
export AWS_ENDPOINT=http://localhost:9000
export AWS_S3_FORCE_PATH_STYLE=true
go run main.go
```

**UI (separate dev server):**
```bash
cd ui
npm run dev
```

### Building

```bash
# Full stack
docker-compose build

# Individual components
docker-compose build filestorage-api
docker-compose build filestorage-ui
```

## API Endpoints

| Method   | Endpoint          | Description             |
|----------|-------------------|-------------------------|
| GET      | `/`               | Welcome + Swagger URL   |
| GET      | `/health`         | Health check            |
| GET      | `/swagger/*`      | Swagger UI documentation|
| GET      | `/files`          | List all files          |
| POST     | `/files`          | Upload files (multipart)|
| GET      | `/files/:filename`| Download file           |
| DELETE   | `/files/:filename`| Delete file             |

**Swagger UI:** http://localhost/swagger/index.html

## Configuration

### Environment Variables (API)

| Variable                  | Required | Default       | Description                      |
|---------------------------|----------|---------------|----------------------------------|
| `S3_REGION`               | Yes      | -             | AWS region (e.g., `us-west-1`)   |
| `S3_BUCKET`               | Yes      | -             | Bucket name                      |
| `AWS_ENDPOINT`            | No       | AWS endpoint  | MinIO/S3 endpoint URL            |
| `AWS_S3_FORCE_PATH_STYLE` | No       | `false`       | `true` for MinIO                 |
| `PORT`                    | No       | `8080`        | API server port                  |

### MinIO Setup

The `minio-setup` service automatically creates the bucket and sets public read policy on startup.

## Constraints

- **Max file size:** 50MB per file
- **Bucket name:** Must exist before API starts (created by `minio-setup`)
- **UI base path:** `/ui/filestorage/` (configured in `vite.config.mjs`)

## Ports

| Service        | Port |
|----------------|------|
| Gateway        | 80   |
| API            | 8080 |
| MinIO API      | 9000 |
| MinIO Console  | 9001 |
| UI Dev Server  | 3000 |

## License
