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
docker build -t ghcr.io/autosubmit-ci:latest .
docker push autosubmit/autosubmit-ci:latest
docker push ghcr.io/autosubmit-ci:latest
```
