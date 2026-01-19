#!/bin/bash

# MediaDash BLE Client - Enhanced Command Processing Test Script
# This script demonstrates the new Redis command queue functionality

set -e

echo "MediaDash BLE Client - Command Processing Test"
echo "=============================================="
echo ""

# Check if redis-cli is available
if ! command -v redis-cli &> /dev/null; then
    echo "Error: redis-cli is not installed. Please install Redis tools to test."
    echo "Ubuntu/Debian: sudo apt install redis-tools"
    echo "macOS: brew install redis"
    exit 1
fi

# Check Redis connection
echo "Testing Redis connection..."
if ! redis-cli -h 127.0.0.1 -p 6379 ping > /dev/null 2>&1; then
    echo "Error: Cannot connect to Redis at 127.0.0.1:6379"
    echo "Please ensure Redis server is running."
    exit 1
fi

echo "✓ Redis connection successful"
echo ""

QUEUE_KEY="system:playback_cmd_q"

# Function to queue a command
queue_command() {
    local action=$1
    local value=${2:-0}
    local timestamp=$(date +%s)
    
    local cmd_json="{\"action\":\"$action\",\"value\":$value,\"timestamp\":$timestamp}"
    
    echo "Queuing command: $action (value: $value)"
    redis-cli -h 127.0.0.1 -p 6379 lpush "$QUEUE_KEY" "$cmd_json" > /dev/null
}

# Function to show queue status
show_queue_status() {
    local queue_length=$(redis-cli -h 127.0.0.1 -p 6379 llen "$QUEUE_KEY")
    echo "Current queue length: $queue_length commands"
    
    if [ "$queue_length" -gt 0 ]; then
        echo "Queued commands (newest first):"
        for i in $(seq 0 $((queue_length - 1))); do
            local cmd=$(redis-cli -h 127.0.0.1 -p 6379 lindex "$QUEUE_KEY" "$i")
            echo "  [$i] $cmd"
        done
    fi
    echo ""
}

# Function to clear queue
clear_queue() {
    echo "Clearing command queue..."
    redis-cli -h 127.0.0.1 -p 6379 del "$QUEUE_KEY" > /dev/null
    echo "✓ Queue cleared"
    echo ""
}

# Main test menu
while true; do
    echo "Enhanced Command Processing Test Options:"
    echo "  1) Queue 'play' command"
    echo "  2) Queue 'pause' command" 
    echo "  3) Queue 'next' command"
    echo "  4) Queue 'previous' command"
    echo "  5) Queue 'volume' command (with value)"
    echo "  6) Queue 'seek' command (with position)"
    echo "  7) Queue 'toggle' command"
    echo "  8) Queue multiple test commands"
    echo "  9) Show queue status"
    echo " 10) Clear queue"
    echo " 11) Monitor Redis keys"
    echo "  q) Quit"
    echo ""
    
    read -p "Select option (1-11, q): " choice
    
    case $choice in
        1)
            queue_command "play"
            ;;
        2)
            queue_command "pause"
            ;;
        3)
            queue_command "next"
            ;;
        4)
            queue_command "previous"
            ;;
        5)
            read -p "Enter volume (0-100): " volume
            if [[ "$volume" =~ ^[0-9]+$ ]] && [ "$volume" -ge 0 ] && [ "$volume" -le 100 ]; then
                queue_command "volume" "$volume"
            else
                echo "Error: Volume must be a number between 0 and 100"
            fi
            ;;
        6)
            read -p "Enter seek position (milliseconds): " position
            if [[ "$position" =~ ^[0-9]+$ ]] && [ "$position" -ge 0 ]; then
                queue_command "seek" "$position"
            else
                echo "Error: Seek position must be a non-negative number"
            fi
            ;;
        7)
            queue_command "toggle"
            ;;
        8)
            echo "Queuing multiple test commands..."
            queue_command "play"
            sleep 0.1
            queue_command "volume" 75
            sleep 0.1
            queue_command "seek" 30000
            sleep 0.1
            queue_command "pause"
            echo "✓ Queued 4 test commands"
            ;;
        9)
            show_queue_status
            ;;
        10)
            clear_queue
            ;;
        11)
            echo "Current MediaDash Redis keys:"
            echo ""
            echo "Media State Keys:"
            redis-cli -h 127.0.0.1 -p 6379 mget \
                media:track media:artist media:album \
                media:playing media:duration media:progress
            echo ""
            echo "System Keys:"
            redis-cli -h 127.0.0.1 -p 6379 mget \
                system:ble_connected system:ble_name
            echo ""
            echo "Command Queue:"
            show_queue_status
            ;;
        q|Q)
            echo "Goodbye!"
            exit 0
            ;;
        *)
            echo "Invalid option. Please try again."
            ;;
    esac
    
    echo ""
done