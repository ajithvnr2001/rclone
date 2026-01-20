# Run this in a Colab cell to execute rclone commands in PARALLEL
# Features: AUTO-RESUME by checking log files, File Size, Transfer Time

import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import re
import os

# ============ CONFIGURATION ============
COMMANDS_FILE = '/content/rclone_commands_largest_first_updated.txt'
LOGS_DIR = '/content/drive/MyDrive/onedrive_migration/logs'
PARALLEL_JOBS = 6   # Number of parallel transfers (2, 4, 8, etc.)
# =======================================

# Thread-safe counters and tracking
lock = threading.Lock()
completed_count = 0
failed_count = 0
skipped_count = 0
total_bytes_transferred = 0
active_transfers = {}

def format_size(size_mb):
    if size_mb >= 1024:
        return f"{size_mb/1024:.2f} GB"
    elif size_mb >= 1:
        return f"{size_mb:.1f} MB"
    else:
        return f"{size_mb*1024:.1f} KB"

def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{int(seconds//60)}m {int(seconds%60)}s"
    else:
        return f"{int(seconds//3600)}h {int((seconds%3600)//60)}m"

def extract_size_from_comment(file_info):
    match = re.search(r'\((\d+\.?\d*)\s*MB\)', file_info)
    if match:
        return float(match.group(1))
    return 0.0

def check_log_completed(log_path):
    """Check if log file exists and shows successful completion"""
    try:
        if os.path.exists(log_path):
            with open(log_path, 'r', encoding='utf-8') as f:
                content = f.read()
                # Check for errors first
                if 'ERROR' in content.upper() or 'FAILED' in content.upper():
                    return False
                # Success indicators
                if '100%' in content or 'Transferred:' in content or 'Elapsed time:' in content:
                    return True
        return False
    except:
        return False

def run_single_command(cmd_info):
    """Run a single rclone command - checks log first, skips if done"""
    global completed_count, failed_count, skipped_count, total_bytes_transferred
    index, total, cmd, file_info = cmd_info
    
    # Extract log file path from command
    log_file = ""
    log_name = ""
    if '--log-file=' in cmd:
        log_file = cmd.split('--log-file=')[1].split()[0]
        log_name = os.path.basename(log_file)
    
    # CHECK LOG FILE FIRST - skip if already completed
    if log_file and check_log_completed(log_file):
        with lock:
            skipped_count += 1
            print(f"⏭️  [{index}] Already done: {log_name}")
        return (index, True, file_info, 0, 0, True)  # skipped=True
    
    # Extract file size
    size_mb = extract_size_from_comment(file_info)
    short_name = file_info.split('(')[0].strip()[:45] if '(' in file_info else file_info[:45]
    
    # Register as active transfer
    with lock:
        active_transfers[index] = {'file': short_name, 'size': size_mb, 'start_time': time.time()}
        active_count = len(active_transfers)
        print(f"▶️  [{index}/{total}] Starting: {short_name}...")
        print(f"   📦 Size: {format_size(size_mb)} | 🔄 Active: {active_count}")
    
    start_time = time.time()
    
    try:
        # Run the command
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        elapsed = time.time() - start_time
        
        with lock:
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
            
            print(f"{status} [{index}/{total}] Completed: {short_name}")
            print(f"   📦 {format_size(size_mb)} | ⏱️ {format_time(elapsed)} | 🚀 {speed}")
            print(f"   📝 Log: {log_name} | ✅ Done: {completed_count} | ❌ Failed: {failed_count}")
            print()
        
        return (index, result.returncode == 0, file_info, size_mb, elapsed, False)
        
    except Exception as e:
        elapsed = time.time() - start_time
        with lock:
            if index in active_transfers:
                del active_transfers[index]
            failed_count += 1
            print(f"❌ [{index}/{total}] Error: {e}")
        return (index, False, str(e), size_mb, elapsed, False)

def run_rclone_parallel(commands_file, max_workers=4):
    """Execute rclone commands in parallel - auto-skips completed by checking logs"""
    global completed_count, failed_count, skipped_count, total_bytes_transferred
    completed_count = 0
    failed_count = 0
    skipped_count = 0
    total_bytes_transferred = 0
    
    # Read and parse all commands
    commands = []
    with open(commands_file, 'r', encoding='utf-8') as f:
        current = 0
        current_file_info = ""
        for line in f:
            line = line.strip()
            
            if line.startswith('# File'):
                if ':' in line:
                    current_file_info = line.split(':', 1)[1].strip()
            
            if line.startswith('!rclone'):
                current += 1
                cmd = line[1:]  # Remove leading '!'
                commands.append((current, cmd, current_file_info))
    
    total = len(commands)
    total_size = sum(extract_size_from_comment(info) for _, _, info in commands)
    
    print(f"🚀 PARALLEL RCLONE EXECUTOR (Auto-Resume via Log Check)")
    print(f"{'='*60}")
    print(f"📁 Commands file: {commands_file.split('/')[-1]}")
    print(f"📊 Total commands: {total}")
    print(f"⚡ Parallel jobs: {max_workers}")
    print(f"📋 Commands to run: {len(commands)}")
    print(f"💾 Total size: {format_size(total_size)}")
    print(f"🔍 Will check log files and skip completed transfers")
    print(f"{'='*60}")
    print()
    
    if not commands:
        print("❌ No commands to run!")
        return
    
    start_time = time.time()
    cmd_infos = [(idx, total, cmd, info) for idx, cmd, info in commands]
    
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(run_single_command, info): info for info in cmd_infos}
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as e:
                    print(f"❌ Task error: {e}")
                    
    except KeyboardInterrupt:
        print(f"\n\n⏸️  INTERRUPTED!")
        print(f"   Completed: {completed_count}, Failed: {failed_count}, Skipped: {skipped_count}")
        print(f"   Just re-run to resume from where you left off!")
        return
    
    elapsed = time.time() - start_time
    
    print()
    print(f"{'='*60}")
    print(f"📊 FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"⏭️  Skipped (already done): {skipped_count}")
    print(f"✅ Newly completed: {completed_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"💾 Data transferred: {format_size(total_bytes_transferred)}")
    print(f"⏱️  Total time: {format_time(elapsed)}")
    if elapsed > 0 and completed_count > 0:
        print(f"🚀 Avg speed: {total_bytes_transferred/elapsed:.2f} MB/s")
        print(f"📈 Files rate: {completed_count*60/elapsed:.1f} files/min")
    
    if failed_count > 0:
        print(f"\n⚠️  {failed_count} transfers failed. Check log files for details.")
    
    if completed_count + skipped_count >= len(commands):
        print(f"\n🎉 ALL TRANSFERS COMPLETED!")

# Run it!
print("🚀 Starting parallel rclone transfers...")
print("🔍 Will automatically skip transfers that have completed log files")
print()
run_rclone_parallel(COMMANDS_FILE, PARALLEL_JOBS)
