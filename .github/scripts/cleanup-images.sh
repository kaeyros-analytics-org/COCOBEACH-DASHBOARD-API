#!/bin/bash
set -e

REGISTRY="${1}"
OWNER="${2}"
IMAGE_NAME="${3}"
SPECIFIC_IMAGE="${4:-}"
SPECIFIC_IMAGE_LATEST="${5:-}"

echo "🧹 Cleaning up Docker images to free up space on self-hosted runner..."

if [[ -n "$SPECIFIC_IMAGE" ]]; then
    echo "Removing specific image: $SPECIFIC_IMAGE"
    docker rmi "$SPECIFIC_IMAGE" 2>/dev/null || true
fi

if [[ -n "$SPECIFIC_IMAGE_LATEST" ]]; then
    echo "Removing latest tag: $SPECIFIC_IMAGE_LATEST"
    docker rmi "$SPECIFIC_IMAGE_LATEST" 2>/dev/null || true
fi

echo "Removing all $IMAGE_NAME images..."
docker images --filter=reference="${REGISTRY}/${OWNER}/${IMAGE_NAME}-*" -q | xargs -r docker rmi -f 2>/dev/null || true

echo "Removing dangling images..."
docker image prune -f 2>/dev/null || true

echo "Cleaning up build cache (older than 24h)..."
docker builder prune -af --filter "until=24h" 2>/dev/null || true

echo "Removing unused images (older than 72h)..."
docker image prune -af --filter "until=72h" 2>/dev/null || true

echo "✅ Cleanup completed. Current images:"
docker images
