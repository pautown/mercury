#!/bin/bash
set -e

# This is a simple build script for cross-compiling the Go client
# for the target armv7 system running Alpine Linux.

echo "Building Go client for linux/armv7..."

# Target architecture for the Car Thing device
export GOOS=linux
export GOARCH=arm
export GOARM=7

# Output binary name
OUTPUT_NAME="mediadash-client"

# Build from project root
cd "$(dirname "$0")/.."

go build -o ./bin/${OUTPUT_NAME} ./cmd/mediadash-client

echo "Build complete! Binary is at golang_ble_client/bin/${OUTPUT_NAME}"
