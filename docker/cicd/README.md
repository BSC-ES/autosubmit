# CICD Docker image

This image is used for CICD in Autosubmit. It contains the dependencies needed
to install and run Autosubmit and its tests.

## Building

```bash
docker build -t autosubmit/autosubmit-ci:latest .
```

## Publishing

First, log in to Docker Hub and GitHub Container Registry:

```bash
docker login
docker login ghcr.io
```

```bash
docker push autosubmit/autosubmit-ci:latest
docker ghcr.io/push bsc-es/autosubmit-ci:latest
```
