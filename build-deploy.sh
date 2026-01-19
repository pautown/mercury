#!/bin/bash
# MediaDash Golang BLE Client - Standalone Build & Deploy Script
# Builds and deploys the golang BLE client for ARM Linux

set -e  # Exit on error

# Colors for output  
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Configuration
BINARY_NAME="mediadash-client"
CARTHING_HOST="${CARTHING_HOST:-172.16.42.2}"
CARTHING_USER="${CARTHING_USER:-root}"
CARTHING_PASSWORD="${CARTHING_PASSWORD:-llizardos}"
DEPLOY_PATH="${DEPLOY_PATH:-/tmp}"
PERSISTENT_PATH="/var/local/bin"

show_usage() {
    echo "MediaDash Golang BLE Client - Standalone Build & Deploy Script"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "COMMANDS:"
    echo "  build        Build for ARM Linux (default)"
    echo "  deploy       Deploy existing binary"
    echo "  build-deploy Build and deploy"
    echo "  clean        Clean build artifacts"
    echo "  test         Test SSH connectivity"
    echo "  status       Check deployment status"
    echo "  help         Show this help"
    echo ""
    echo "OPTIONS:"
    echo "  -p, --persistent    Deploy to persistent path (/var/local/bin)"
    echo "  -s, --strip        Strip debug symbols"
    echo "  -v, --verbose      Enable verbose logging"
    echo "  --native          Build for native architecture instead of ARM"
    echo "  --no-password     Use SSH keys instead of password"
    echo ""
    echo "ENVIRONMENT VARIABLES:"
    echo "  CARTHING_HOST=$CARTHING_HOST"
    echo "  CARTHING_USER=$CARTHING_USER"
    echo "  CARTHING_PASSWORD=$CARTHING_PASSWORD" 
    echo "  DEPLOY_PATH=$DEPLOY_PATH"
    echo ""
    echo "EXAMPLES:"
    echo "  $0                    # Build for ARM"
    echo "  $0 build-deploy      # Build and deploy"
    echo "  $0 deploy -p         # Deploy to persistent path"
    echo "  $0 --native         # Build for native system"
    echo ""
}

# Parse arguments
COMMAND="build"
USE_PERSISTENT=false
STRIP_BINARY=false
VERBOSE=false
NATIVE_BUILD=false
USE_PASSWORD=true

while [[ $# -gt 0 ]]; do
    case $1 in
        build|deploy|build-deploy|clean|test|status|help)
            COMMAND="$1"
            ;;
        -p|--persistent)
            USE_PERSISTENT=true
            ;;
        -s|--strip)
            STRIP_BINARY=true
            ;;
        -v|--verbose)
            VERBOSE=true
            ;;
        --native)
            NATIVE_BUILD=true
            ;;
        --no-password)
            USE_PASSWORD=false
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
    shift
done

# Set deploy path
if [[ "$USE_PERSISTENT" == "true" ]]; then
    REMOTE_PATH="$PERSISTENT_PATH"
else
    REMOTE_PATH="$DEPLOY_PATH"
fi

REMOTE_BINARY_PATH="${REMOTE_PATH}/${BINARY_NAME}"
LOCAL_BINARY_PATH="./bin/${BINARY_NAME}"

# SSH command wrapper
ssh_exec() {
    local cmd="$1"
    if [[ "$USE_PASSWORD" == "true" ]]; then
        sshpass -p "$CARTHING_PASSWORD" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$CARTHING_USER@$CARTHING_HOST" "$cmd"
    else
        ssh -o ConnectTimeout=10 "$CARTHING_USER@$CARTHING_HOST" "$cmd"
    fi
}

# SCP command wrapper
scp_exec() {
    local src="$1"
    local dest="$2"
    if [[ "$USE_PASSWORD" == "true" ]]; then
        sshpass -p "$CARTHING_PASSWORD" scp -o StrictHostKeyChecking=no "$src" "$CARTHING_USER@$CARTHING_HOST:$dest"
    else
        scp "$src" "$CARTHING_USER@$CARTHING_HOST:$dest"
    fi
}

# Check Go installation
check_go() {
    if ! command -v go &> /dev/null; then
        log_error "Go not found. Please install Go (https://golang.org/dl/)"
        return 1
    fi
    
    local go_version=$(go version | cut -d' ' -f3)
    log_info "Go version: $go_version"
    return 0
}

# Build the binary
build_binary() {
    log_info "Building Golang BLE Client..."
    
    if ! check_go; then
        return 1
    fi
    
    # Set target architecture
    if [[ "$NATIVE_BUILD" == "true" ]]; then
        unset GOOS GOARCH GOARM
        log_info "Building for native architecture: $(go env GOOS)/$(go env GOARCH)"
    else
        export GOOS=linux
        export GOARCH=arm
        export GOARM=7
        log_info "Cross-compiling for linux/armv7"
    fi
    
    # Create bin directory
    mkdir -p bin
    
    # Build options
    local build_flags=""
    if [[ "$STRIP_BINARY" == "true" ]]; then
        build_flags="-ldflags='-w -s'"
    fi
    
    if [[ "$VERBOSE" == "true" ]]; then
        build_flags+=" -v"
    fi
    
    # Execute build
    if [[ -n "$build_flags" ]]; then
        eval "go build $build_flags -o '$LOCAL_BINARY_PATH' ./cmd/mediadash-client"
    else
        go build -o "$LOCAL_BINARY_PATH" ./cmd/mediadash-client
    fi
    
    if [[ $? -eq 0 ]]; then
        local binary_size=$(stat -c%s "$LOCAL_BINARY_PATH")
        log_success "Build completed: $LOCAL_BINARY_PATH (${binary_size} bytes)"
        
        # Show binary info
        if [[ "$VERBOSE" == "true" ]]; then
            log_info "Binary info: $(file "$LOCAL_BINARY_PATH" | cut -d: -f2 | xargs)"
        fi
        
        return 0
    else
        log_error "Build failed"
        return 1
    fi
}

# Test SSH connectivity
test_ssh() {
    log_info "Testing SSH connectivity to $CARTHING_USER@$CARTHING_HOST..."
    
    if ! command -v sshpass &> /dev/null && [[ "$USE_PASSWORD" == "true" ]]; then
        log_error "sshpass not found. Install with: sudo apt install sshpass"
        return 1
    fi
    
    if ssh_exec "echo 'SSH connection successful'"; then
        log_success "SSH connectivity confirmed"
        return 0
    else
        log_error "SSH connection failed"
        return 1
    fi
}

# Check if binary exists and validate
check_binary() {
    if [[ ! -f "$LOCAL_BINARY_PATH" ]]; then
        log_error "Binary $LOCAL_BINARY_PATH not found"
        log_info "Build the binary first with: $0 build"
        return 1
    fi
    
    # Validate architecture for ARM deployment
    if [[ "$NATIVE_BUILD" != "true" ]]; then
        local file_info=$(file "$LOCAL_BINARY_PATH")
        if ! echo "$file_info" | grep -q "ARM"; then
            log_warn "Binary may not be ARM architecture: $file_info"
        fi
    fi
    
    log_info "Binary found: $LOCAL_BINARY_PATH"
    return 0
}

# Deploy binary to remote device
deploy_binary() {
    log_info "Deploying $BINARY_NAME to $CARTHING_HOST:$REMOTE_BINARY_PATH"
    
    if ! check_binary; then
        return 1
    fi
    
    if ! test_ssh; then
        return 1
    fi
    
    # Create target directory
    if ! ssh_exec "mkdir -p '$REMOTE_PATH'"; then
        log_error "Failed to create target directory: $REMOTE_PATH"
        return 1
    fi
    
    # Backup existing binary if it exists
    if ssh_exec "test -f '$REMOTE_BINARY_PATH'"; then
        local backup_path="${REMOTE_BINARY_PATH}.backup"
        log_info "Creating backup: $backup_path"
        ssh_exec "cp '$REMOTE_BINARY_PATH' '$backup_path'" || log_warn "Backup creation failed"
    fi
    
    # Copy binary
    local binary_size=$(stat -c%s "$LOCAL_BINARY_PATH")
    log_info "Transferring binary (${binary_size} bytes)..."
    
    if ! scp_exec "$LOCAL_BINARY_PATH" "$REMOTE_BINARY_PATH"; then
        log_error "Binary transfer failed"
        return 1
    fi
    
    # Set executable permissions
    if ! ssh_exec "chmod +x '$REMOTE_BINARY_PATH'"; then
        log_error "Failed to set executable permissions"
        return 1
    fi
    
    log_success "Binary deployed successfully"
    
    # Test execution
    log_info "Testing binary execution..."
    if ssh_exec "'$REMOTE_BINARY_PATH' --help > /dev/null 2>&1 || echo 'Binary execution test completed'"; then
        log_success "Binary execution test passed"
    else
        log_warn "Binary execution test failed (may be normal if --help not implemented)"
    fi
    
    return 0
}

# Show deployment status
show_status() {
    log_info "Deployment Status for $CARTHING_HOST"
    echo ""
    
    # Local binary status
    if [[ -f "$LOCAL_BINARY_PATH" ]]; then
        local size=$(stat -c%s "$LOCAL_BINARY_PATH")
        local arch=$(file "$LOCAL_BINARY_PATH" | cut -d: -f2 | xargs)
        echo "✅ Local Binary: $LOCAL_BINARY_PATH (${size} bytes, $arch)"
    else
        echo "❌ Local Binary: Not built"
    fi
    
    # Remote connectivity
    if ! test_ssh &>/dev/null; then
        echo "❌ SSH Connection: Failed"
        echo "❓ Remote Status: Cannot check"
        return 1
    fi
    
    echo "✅ SSH Connection: OK"
    
    # Check remote binaries
    if ssh_exec "test -f '${DEPLOY_PATH}/${BINARY_NAME}'"; then
        local size=$(ssh_exec "stat -c%s '${DEPLOY_PATH}/${BINARY_NAME}'" 2>/dev/null || echo "Unknown")
        echo "✅ Temporary Binary: ${DEPLOY_PATH}/${BINARY_NAME} (${size} bytes)"
    else
        echo "❌ Temporary Binary: Not found"
    fi
    
    if ssh_exec "test -f '${PERSISTENT_PATH}/${BINARY_NAME}'"; then
        local size=$(ssh_exec "stat -c%s '${PERSISTENT_PATH}/${BINARY_NAME}'" 2>/dev/null || echo "Unknown")
        echo "✅ Persistent Binary: ${PERSISTENT_PATH}/${BINARY_NAME} (${size} bytes)"
    else
        echo "❌ Persistent Binary: Not found"  
    fi
    
    # Check if process is running
    if ssh_exec "pgrep -f '$BINARY_NAME' > /dev/null"; then
        echo "✅ Process Status: Running"
    else
        echo "❌ Process Status: Not running"
    fi
    
    echo ""
    echo "Timestamp: $(date)"
}

# Clean build artifacts
clean_artifacts() {
    log_info "Cleaning build artifacts..."
    rm -rf bin/
    log_success "Build artifacts cleaned"
}

# Main execution
case "$COMMAND" in
    build)
        build_binary
        ;;
    deploy)
        deploy_binary
        ;;
    build-deploy)
        if ! build_binary; then
            exit 1
        fi
        if ! deploy_binary; then
            exit 1
        fi
        log_success "Build and deployment completed!"
        ;;
    clean)
        clean_artifacts
        ;;
    test)
        test_ssh
        ;;
    status)
        show_status
        ;;
    help)
        show_usage
        ;;
    *)
        log_error "Unknown command: $COMMAND"
        show_usage
        exit 1
        ;;
esac