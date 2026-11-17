# Agent Instructions

## Running the Service

```bash
# Full stack via Docker Compose
docker-compose up --build

# Individual services
docker-compose up filestorage-api
docker-compose up filestorage-ui
```

## Development Commands

**API (Go):**
- `cd api && go run main.go` - Run API server locally (requires MinIO and env vars)
- `swag init` - Regenerate Swagger docs (run after API changes)

**UI (Vue/Vite):**
- `cd ui && npm run dev` - Start Vite dev server on port 3000
- `cd ui && npm run build` - Build for production

## Architecture

**Components:**
- `api/` - Go Echo microservice (S3/MinIO file storage)
- `ui/` - Vue 3 + Vuetify 3 frontend
- `nginx/` - OpenResty reverse gateway (port 80)
- `minio` - S3-compatible storage (port 9000/9001)

**Routing (via gateway on port 80):**
- `/` → UI (rewritten to `/ui/filestorage/`)
- `/api/filestorage/*` → API (`/files`, `/health`, `/swagger/*`)

**API Entry:** `api/main.go` - initializes S3 client, registers routes

## Environment Variables (API)

Required: `S3_REGION`, `S3_BUCKET`
Optional: `AWS_ENDPOINT`, `AWS_S3_FORCE_PATH_STYLE`, `PORT`

## Key Constraints

- **File size limit:** 50MB per file (enforced in `uploadFilesHandler`)
- **Swagger docs:** Generate with `swag init` before building Docker image
- **UI base path:** `/ui/filestorage/` (configured in `vite.config.mjs`)
- **MinIO bucket:** Must be created by `minio-setup` service before API starts

## API Endpoints

- `GET /` - Welcome + Swagger URL
- `GET /health` - Health check
- `GET /swagger/*` - Swagger UI
- `GET /files` - List files
- `POST /files` - Upload files (multipart)
- `GET /files/:filename` - Download file
- `DELETE /files/:filename` - Delete file

## Swagger

Generated docs in `api/docs/`. UI at `/swagger/index.html` (via gateway).
