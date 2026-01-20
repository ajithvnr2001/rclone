# Run this in a Colab cell to execute rclone commands in PARALLEL
# Features: File Size, Transfer Time, Active Transfers display

import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import re

# ============ CONFIGURATION ============
COMMANDS_FILE = '/content/drive/MyDrive/onedrive_migration/rclone_commands_largest_first.txt'
START_FROM = 1      # Start from command number (1-indexed, use this to resume)
PARALLEL_JOBS = 4   # Number of parallel transfers (2, 4, 8, etc.)
# =======================================

# Thread-safe counters and tracking
lock = threading.Lock()
completed_count = 0
failed_count = 0
total_bytes_transferred = 0
active_transfers = {}  # Track currently running transfers

def format_size(size_mb):
    """Format size in human-readable format"""
    if size_mb >= 1024:
        return f"{size_mb/1024:.2f} GB"
    elif size_mb >= 1:
        return f"{size_mb:.1f} MB"
    else:
        return f"{size_mb*1024:.1f} KB"

def format_time(seconds):
    """Format seconds into human-readable time"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{int(seconds//60)}m {int(seconds%60)}s"
    else:
        return f"{int(seconds//3600)}h {int((seconds%3600)//60)}m"

def extract_size_from_comment(file_info):
    """Extract size in MB from file info comment like 'filename (123.45 MB)'"""
    match = re.search(r'\((\d+\.?\d*)\s*MB\)', file_info)
    if match:
        return float(match.group(1))
    return 0.0

def show_active_transfers():
    """Display currently active transfers"""
    with lock:
        if active_transfers:
            print(f"\n🔄 ACTIVE TRANSFERS ({len(active_transfers)}):")
            for idx, info in active_transfers.items():
                elapsed = time.time() - info['start_time']
                print(f"   [{idx}] {info['file'][:40]}... ({format_time(elapsed)})")
            print()

def run_single_command(cmd_info):
    """Run a single rclone command with timing and size tracking"""
    global completed_count, failed_count, total_bytes_transferred
    index, total, cmd, file_info = cmd_info
    
    # Extract file size from comment
    size_mb = extract_size_from_comment(file_info)
    
    # Extract log file path from command
    log_file = "unknown"
    if '--log-file=' in cmd:
        log_file = cmd.split('--log-file=')[1].split()[0]
    
    # Extract short filename
    short_name = file_info.split('(')[0].strip()[:45] if '(' in file_info else file_info[:45]
    
    # Register as active transfer
    with lock:
        active_transfers[index] = {
            'file': short_name,
            'size': size_mb,
            'start_time': time.time()
        }
        active_count = len(active_transfers)
        print(f"▶️  [{index}/{total}] Starting: {short_name}...")
        print(f"   📦 Size: {format_size(size_mb)} | 🔄 Active: {active_count}")
    
    start_time = time.time()
    
    try:
        # Run the command using subprocess
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True
        )
        
        elapsed = time.time() - start_time
        
        with lock:
            # Remove from active transfers
            if index in active_transfers:
                del active_transfers[index]
            
            if result.returncode == 0:
                completed_count += 1
                total_bytes_transferred += size_mb
                status = "✅"
                speed = f"{size_mb/elapsed:.2f} MB/s" if elapsed > 0 and size_mb > 0 else "N/A"
            else:
                failed_count += 1
                status = "❌"
                speed = "Failed"
            
            log_name = log_file.split('/')[-1]
            print(f"{status} [{index}/{total}] Completed: {short_name}")
            print(f"   📦 {format_size(size_mb)} | ⏱️ {format_time(elapsed)} | 🚀 {speed}")
            print(f"   📝 Log: {log_name} | ✅ Done: {completed_count} | ❌ Failed: {failed_count}")
            print()
        
        return (index, result.returncode == 0, file_info, log_file, size_mb, elapsed)
        
    except Exception as e:
        elapsed = time.time() - start_time
        with lock:
            if index in active_transfers:
                del active_transfers[index]
            failed_count += 1
            print(f"❌ [{index}/{total}] Error: {e}")
        return (index, False, str(e), log_file, size_mb, elapsed)

def run_rclone_parallel(commands_file, start_from=1, max_workers=4):
    """Execute rclone commands in parallel with enhanced tracking"""
    global completed_count, failed_count, total_bytes_transferred
    completed_count = 0
    failed_count = 0
    total_bytes_transferred = 0
    
    # Read and parse all commands
    commands = []
    with open(commands_file, 'r', encoding='utf-8') as f:
        current = 0
        current_file_info = ""
        for line in f:
            line = line.strip()
            
            # Capture file info from comment (includes size)
            if line.startswith('# File'):
                # Format: "# File 1: filename (123.45 MB)"
                if ':' in line:
                    current_file_info = line.split(':', 1)[1].strip()
            
            # Process rclone commands
            if line.startswith('!rclone'):
                current += 1
                if current >= start_from:
                    cmd = line[1:]  # Remove leading '!'
                    commands.append((current, cmd, current_file_info))
    
    total = len(commands) + start_from - 1
    
    # Calculate total size to transfer
    total_size = sum(extract_size_from_comment(info) for _, _, info in commands)
    
    print(f"🚀 PARALLEL RCLONE EXECUTOR (Enhanced)")
    print(f"{'='*60}")
    print(f"📁 Commands file: {commands_file.split('/')[-1]}")
    print(f"📊 Total commands: {total}")
    print(f"▶️  Starting from: {start_from}")
    print(f"⚡ Parallel jobs: {max_workers}")
    print(f"📋 Commands to run: {len(commands)}")
    print(f"💾 Total size: {format_size(total_size)}")
    print(f"{'='*60}")
    print()
    
    if not commands:
        print("❌ No commands to run!")
        return
    
    start_time = time.time()
    
    # Prepare command info tuples
    cmd_infos = [(idx, total, cmd, info) for idx, cmd, info in commands]
    
    try:
        # Run commands in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(run_single_command, info): info for info in cmd_infos}
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as e:
                    print(f"❌ Task error: {e}")
                    
    except KeyboardInterrupt:
        print(f"\n\n⏸️  INTERRUPTED!")
        print(f"   Completed: {completed_count}, Failed: {failed_count}")
        print(f"   To resume, set START_FROM = {start_from + completed_count}")
        return
    
    elapsed = time.time() - start_time
    
    print()
    print(f"{'='*60}")
    print(f"📊 FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Completed: {completed_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"💾 Data transferred: {format_size(total_bytes_transferred)}")
    print(f"⏱️  Total time: {format_time(elapsed)}")
    if elapsed > 0:
        print(f"🚀 Avg speed: {total_bytes_transferred/elapsed:.2f} MB/s")
        print(f"📈 Files rate: {completed_count*60/elapsed:.1f} files/min")
    
    if failed_count > 0:
        print(f"\n⚠️  {failed_count} transfers failed. Check log files for details.")

# Run it!
print("🚀 Starting parallel rclone transfers with enhanced tracking...")
print()
run_rclone_parallel(COMMANDS_FILE, START_FROM, PARALLEL_JOBS)
