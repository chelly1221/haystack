# Docker Integration for File Processing Service

## Architecture

The file processing system now runs as a containerized microservice architecture:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   haystack-app  │    │ file-processor  │    │     qdrant      │
│                 │    │                 │    │                 │
│ FastAPI Server  │    │ File Watcher    │    │ Vector Database │
│ /upload-simple/ │───▶│ Processing      │    │                 │
│ /processor-     │    │ Task Management │    │                 │
│ tasks/ API      │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │ Shared Volumes  │
                    │                 │
                    │ • uploads/      │
                    │ • data/         │
                    │ • logs/         │
                    └─────────────────┘
```

## Services

### haystack-app
- **Purpose**: Main FastAPI application
- **Endpoints**: `/upload-simple/`, `/processor-tasks/`
- **Dependencies**: qdrant, vllm-server
- **GPU**: Uses NVIDIA GPU 1

### file-processor
- **Purpose**: Independent file processing service
- **Function**: Watches upload directory, processes files
- **Dependencies**: qdrant (for future document storage)
- **Database**: SQLite for task management

### qdrant
- **Purpose**: Vector database for document embeddings
- **Shared**: Used by both services

## Shared Volumes

- **uploads/**: File transfer between services
- **file_processing_data/**: Shared task database
- **processor_logs/**: Processing service logs

## Deployment Commands

### Start all services:
```bash
docker-compose up -d
```

### Start specific services:
```bash
docker-compose up -d qdrant file-processor haystack-app
```

### View logs:
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f file-processor
```

### Restart processor only:
```bash
docker-compose restart file-processor
```

### Scale processor (if needed):
```bash
docker-compose up -d --scale file-processor=2
```

## Health Checks

- **file-processor**: Checks for task database file
- **Interval**: 30 seconds
- **Timeout**: 10 seconds
- **Start period**: 5 seconds

## Environment Variables

### file-processor
- `PYTHONUNBUFFERED=1`: Real-time logs
- `LOG_LEVEL=INFO`: Logging verbosity

## Troubleshooting

### Check processor status:
```bash
docker-compose ps file-processor
```

### View processor health:
```bash
docker inspect --format='{{json .State.Health}}' file-processor
```

### Access processor container:
```bash
docker exec -it file-processor bash
```

### Manual processor restart:
```bash
docker-compose stop file-processor
docker-compose start file-processor
```

## Benefits of Containerization

1. **Isolation**: Services run independently
2. **Scalability**: Can scale processor separately
3. **Management**: Easy start/stop/restart
4. **Monitoring**: Built-in health checks
5. **Consistency**: Same environment everywhere
6. **Recovery**: Auto-restart on failure