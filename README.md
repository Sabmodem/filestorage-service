# Simple filestorage app

The app interacts with S3 and the similar storages. The app in current example uses minio, but you can use s3

## Usage
```
Bash
./docker compose up -d # run the infrastructure
```

# Testing
- Open the browser and go to ```http://localhost:8080/swagger/index.html``` to get a swagget interactive documentation
- Open the browser and go to ```http://localhost:80``` to load a vuetify frontend that interacts with app