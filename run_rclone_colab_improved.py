# Run this in a Colab cell to execute rclone commands in PARALLEL
# Supports running 2, 4, or more transfers simultaneously

import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

# ============ CONFIGURATION ============
COMMANDS_FILE = '/content/drive/MyDrive/onedrive_migration/rclone_commands_largest_first.txt'
START_FROM = 1      # Start from command number (1-indexed, use this to resume)
PARALLEL_JOBS = 4   # Number of parallel transfers (2, 4, 8, etc.)
# =======================================

# Thread-safe counter
lock = threading.Lock()
completed_count = 0
failed_count = 0

def run_single_command(cmd_info):
    """Run a single rclone command"""
    global completed_count, failed_count
    index, total, cmd, file_info = cmd_info
    
    # Extract log file path from command
    log_file = "unknown"
    if '--log-file=' in cmd:
        log_file = cmd.split('--log-file=')[1].split()[0]
    
    try:
        # Run the command using subprocess (without the leading !)
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True
        )
        
        with lock:
            if result.returncode == 0:
                completed_count += 1
                status = "✅"
            else:
                failed_count += 1
                status = "❌"
            # Show file info and log file location
            short_info = file_info[:50] + "..." if len(file_info) > 50 else file_info
            log_name = log_file.split('/')[-1]  # Just the filename
            print(f"{status} [{index}/{total}] {short_info}")
            print(f"   📝 Log: {log_name}")
        
        return (index, result.returncode == 0, file_info, log_file)
        
    except Exception as e:
        with lock:
            failed_count += 1
            print(f"❌ [{index}/{total}] Error: {e}")
        return (index, False, str(e), log_file)

def run_rclone_parallel(commands_file, start_from=1, max_workers=4):
    """Execute rclone commands in parallel"""
    global completed_count, failed_count
    completed_count = 0
    failed_count = 0
    
    # Read and parse all commands
    commands = []
    with open(commands_file, 'r', encoding='utf-8') as f:
        current = 0
        current_file_info = ""
        for line in f:
            line = line.strip()
            
            # Capture file info from comment
            if line.startswith('# File'):
                current_file_info = line.replace('# File ', '').split(':')[1].strip() if ':' in line else line
            
            # Process rclone commands
            if line.startswith('!rclone'):
                current += 1
                if current >= start_from:
                    # Remove the leading '!'
                    cmd = line[1:]
                    commands.append((current, cmd, current_file_info))
    
    total = len(commands) + start_from - 1
    
    print(f"🚀 PARALLEL RCLONE EXECUTOR")
    print(f"=" * 50)
    print(f"📁 Commands file: {commands_file}")
    print(f"📊 Total commands: {total}")
    print(f"▶️  Starting from: {start_from}")
    print(f"⚡ Parallel jobs: {max_workers}")
    print(f"📋 Commands to run: {len(commands)}")
    print(f"=" * 50)
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
    print(f"=" * 50)
    print(f"📊 SUMMARY")
    print(f"=" * 50)
    print(f"✅ Completed: {completed_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"⏱️  Time: {elapsed/60:.1f} minutes")
    print(f"🚀 Speed: {completed_count / (elapsed/60):.1f} files/minute")
    
    if failed_count > 0:
        print(f"\n⚠️  Some transfers failed. Check log files for details.")

# Run it!
print("Starting parallel rclone transfers...")
run_rclone_parallel(COMMANDS_FILE, START_FROM, PARALLEL_JOBS)
