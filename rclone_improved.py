import subprocess
import json
import os
import sys
from pathlib import Path

# Hardcoded configuration
RCLONE_CONFIG_PATH = "/content/rclone.conf"
REMOTE_NAME = "onedrive:"
DESTINATION_REMOTE = "drivemig:Data_Migration/"
OUTPUT_DIR = "/content/drive/MyDrive/onedrive_migration"
LOG_DIR = "/content/drive/MyDrive/onedrive_migration/logs"

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# rclone parameters for files above 100MB
HIGH_PERFORMANCE_PARAMS = "--transfers=64 --checkers=128 --multi-thread-streams=16 --buffer-size=2G --stats=30s --tpslimit=0 --tpslimit-burst=0 --bwlimit=0 --low-level-retries=10 --timeout=5m --retries=10"

def print_progress(current, total, prefix='Progress:', suffix='', decimals=1, length=50):
    """
    Create and display a progress bar
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (current / float(total)))
    filled_length = int(length * current // total)
    bar = '█' * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {current}/{total} {percent}% {suffix}', end='', flush=True)

def extract_file_path_from_format(formatted_entry, source_remote=REMOTE_NAME):
    """
    Extract file path from format: "ajithvnr2001i"/"folder"/"file.txt"/"size"
    Handles subfolders in remote name properly
    """
    # Remove quotes and split by "/"
    parts = formatted_entry.replace('"', '').split('/')

    if len(parts) < 2:
        return None

    # Parse the source remote to get remote name and base folder
    if ':' in source_remote:
        remote_base, base_folder = source_remote.split(':', 1)
    else:
        remote_base = source_remote
        base_folder = ""

    # First part is remote name, last part is size, middle parts are file path
    file_path_parts = parts[1:-1]  # Exclude remote name and size

    # Join path parts
    relative_path = '/'.join(file_path_parts)

    # Construct full source path
    if base_folder:
        # If there's a base folder in remote, include it
        full_path = f"{remote_base}:{base_folder}/{relative_path}" if relative_path else f"{remote_base}:{base_folder}"
    else:
        # If no base folder, just use the relative path
        full_path = f"{remote_base}:{relative_path}" if relative_path else f"{remote_base}:"

    return full_path

def generate_rclone_commands(file_entries):
    """
    Generate rclone copy commands for each file with proper path handling
    """
    print("\n🔧 Generating rclone copy commands...")
    print(f"📁 Source: {REMOTE_NAME}")
    print(f"🎯 Destination: {DESTINATION_REMOTE}")
    print("=" * 60)

    commands = []
    large_file_count = 0
    small_file_count = 0

    for i, (formatted_entry, size_mb) in enumerate(file_entries):
        # Extract source path with proper subfolder handling
        source_path = extract_file_path_from_format(formatted_entry, REMOTE_NAME)
        if not source_path:
            print(f"⚠️  Skipping invalid entry: {formatted_entry}")
            continue

        # Create destination path
        # Extract relative path from source for destination
        if ':' in source_path:
            source_parts = source_path.split(':', 1)
            if len(source_parts) > 1 and source_parts[1]:
                # Remove base folder from source path for destination
                source_relative = source_parts[1]
                if REMOTE_NAME.count(':') > 0 and '/' in REMOTE_NAME.split(':', 1)[1]:
                    # Remove the base folder part
                    base_folder = REMOTE_NAME.split(':', 1)[1]
                    if source_relative.startswith(base_folder + '/'):
                        source_relative = source_relative[len(base_folder) + 1:]
                    elif source_relative == base_folder:
                        source_relative = ""

                dest_path = f"{DESTINATION_REMOTE}{source_relative}" if source_relative else DESTINATION_REMOTE.rstrip(':')
            else:
                dest_path = DESTINATION_REMOTE.rstrip(':')
        else:
            dest_path = DESTINATION_REMOTE.rstrip(':')

        # Determine log file name with full path
        if i == 0:
            log_file = f"{LOG_DIR}/rclone_output.log"
        else:
            log_file = f"{LOG_DIR}/rclone_output{i:02d}.log"

        # Choose parameters based on file size
        if size_mb >= 100:
            # Large files (>= 100MB) - use high performance parameters
            cmd = f'!rclone --config={RCLONE_CONFIG_PATH} copy -P "{source_path}" "{dest_path}" {HIGH_PERFORMANCE_PARAMS} --log-level INFO --log-file={log_file}'
            large_file_count += 1
        else:
            # Small files (< 100MB) - use default parameters
            cmd = f'!rclone --config={RCLONE_CONFIG_PATH} copy -P "{source_path}" "{dest_path}" --log-level INFO --log-file={log_file}'
            small_file_count += 1

        # Extract display name for logging
        display_name = source_relative if 'source_relative' in locals() else source_path.split(':')[1] if ':' in source_path else 'unknown'
        commands.append((cmd, size_mb, display_name))

    print(f"\n📊 Command Generation Summary:")
    print(f"• Large files (≥100MB): {large_file_count} commands with high-performance parameters")
    print(f"• Small files (<100MB): {small_file_count} commands with default parameters")
    print(f"• Total commands: {len(commands)}")

    return commands

def save_commands_to_file(commands):
    """
    Save generated commands to files:
    - All commands (original order)
    - Largest files first (sorted descending by size)
    - Smallest files first (sorted ascending by size)
    Writes to local /content/ first, then copies to Google Drive
    """
    print(f"\n💾 Saving commands locally, then copying to: {OUTPUT_DIR}")

    from datetime import datetime
    import shutil
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    local_files = []

    # 1. Save ALL commands (original order as discovered) - LOCAL
    local_all = 'rclone_commands_all.txt'
    local_files.append(local_all)
    with open(local_all, 'w', encoding='utf-8') as f:
        f.write("# Rclone Copy Commands - ALL FILES (Original Order)\n")
        f.write(f"# Generated on: {timestamp}\n")
        f.write(f"# Source: {REMOTE_NAME}\n")
        f.write(f"# Destination: {DESTINATION_REMOTE}\n")
        f.write(f"# Total files: {len(commands)}\n")
        f.write("# Large files (≥100MB) use high-performance parameters\n\n")

        for i, (cmd, size_mb, file_path) in enumerate(commands):
            f.write(f"# File {i+1}: {file_path} ({size_mb} MB)\n")
            f.write(f"{cmd}\n\n")

    # 2. Save LARGEST files first - LOCAL
    largest_first = sorted(commands, key=lambda x: x[1], reverse=True)
    local_largest = 'rclone_commands_largest_first.txt'
    local_files.append(local_largest)
    with open(local_largest, 'w', encoding='utf-8') as f:
        f.write("# Rclone Copy Commands - LARGEST FILES FIRST\n")
        f.write(f"# Generated on: {timestamp}\n")
        f.write(f"# Source: {REMOTE_NAME}\n")
        f.write(f"# Destination: {DESTINATION_REMOTE}\n")
        f.write(f"# Total files: {len(commands)}\n")
        f.write("# Sorted by size: LARGEST → SMALLEST\n\n")

        for i, (cmd, size_mb, file_path) in enumerate(largest_first):
            f.write(f"# File {i+1}: {file_path} ({size_mb} MB)\n")
            f.write(f"{cmd}\n\n")

    # 3. Save SMALLEST files first - LOCAL
    smallest_first = sorted(commands, key=lambda x: x[1], reverse=False)
    local_smallest = 'rclone_commands_smallest_first.txt'
    local_files.append(local_smallest)
    with open(local_smallest, 'w', encoding='utf-8') as f:
        f.write("# Rclone Copy Commands - SMALLEST FILES FIRST\n")
        f.write(f"# Generated on: {timestamp}\n")
        f.write(f"# Source: {REMOTE_NAME}\n")
        f.write(f"# Destination: {DESTINATION_REMOTE}\n")
        f.write(f"# Total files: {len(commands)}\n")
        f.write("# Sorted by size: SMALLEST → LARGEST\n\n")

        for i, (cmd, size_mb, file_path) in enumerate(smallest_first):
            f.write(f"# File {i+1}: {file_path} ({size_mb} MB)\n")
            f.write(f"{cmd}\n\n")

    # Copy all files to Google Drive
    print("📤 Copying to Google Drive...")
    for local_file in local_files:
        shutil.copy2(local_file, os.path.join(OUTPUT_DIR, local_file))

    print(f"✅ Commands saved locally (/content/) and copied to {OUTPUT_DIR}:")
    print(f"  📄 rclone_commands_all.txt          ({len(commands)} commands - original order)")
    print(f"  📄 rclone_commands_largest_first.txt  ({len(commands)} commands - largest → smallest)")
    print(f"  📄 rclone_commands_smallest_first.txt ({len(commands)} commands - smallest → largest)")

def list_files_with_rclone_realtime(remote_path=REMOTE_NAME):
    """
    Use rclone to list all files with REAL-TIME STREAMING output (like manual command)
    Uses 'rclone ls' for streaming, same format as manual command
    """
    try:
        print("🔄 Starting rclone file listing (STREAMING MODE)...")
        print(f"📁 Listing from: {remote_path}")

        # Use 'rclone ls' for streaming output (same as manual command)
        cmd = [
            "rclone",
            "ls",
            "--config", RCLONE_CONFIG_PATH,
            remote_path
        ]

        print(f"📡 Running command: {' '.join(cmd)}")
        print("⏳ Streaming file list from remote (real-time)...")
        print("-" * 60)

        file_entries = []
        file_count = 0
        error_lines = []

        # Extract remote name for formatting
        remote_name_only = remote_path.split(':')[0] if ':' in remote_path else remote_path

        # Use Popen for real-time streaming output (NOT subprocess.run!)
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line buffered
            universal_newlines=True
        )

        # Open file for real-time writing
        with open('folder_detailes.txt', 'w', encoding='utf-8') as f:
            # Read stdout line by line in real-time
            for line in process.stdout:
                line = line.strip()
                if not line:
                    continue

                # Parse rclone ls output format: "    SIZE PATH"
                # Example: "    46080 Work Files/ABBREVIATIONS.xls"
                parts = line.split(None, 1)  # Split on first whitespace
                if len(parts) == 2:
                    try:
                        file_size = int(parts[0])
                        file_path = parts[1]

                        # Convert size from bytes to MB
                        size_in_mb = round(file_size / (1024 * 1024), 2)

                        # Split the path into components
                        path_parts = file_path.split('/')

                        # Create the formatted string
                        if len(path_parts) > 1:
                            formatted_path = f'"{remote_name_only}"'
                            for part in path_parts[:-1]:
                                formatted_path += f'/"{part}"'
                            formatted_path += f'/"{path_parts[-1]}"/"{size_in_mb}"'
                        else:
                            formatted_path = f'"{remote_name_only}"/"{path_parts[0]}"/"{size_in_mb}"'

                        file_entries.append((formatted_path, size_in_mb))
                        file_count += 1

                        # INSTANT WRITE: Write to file immediately as each file is found
                        f.write(formatted_path + '\n')
                        f.flush()  # Force write to disk immediately

                        # STREAM: Print each file as it's found (real-time output!)
                        display_path = file_path[:60] + "..." if len(file_path) > 60 else file_path
                        print(f"  [{file_count:5d}] {size_in_mb:>10.2f} MB | {display_path}")

                    except ValueError:
                        # Skip lines that don't match expected format
                        pass

        # Collect any stderr messages
        stderr_output = process.stderr.read()
        if stderr_output:
            error_lines = stderr_output.strip().split('\n')

        # Wait for process to complete
        process.wait()

        # Show warnings if any
        if error_lines:
            print(f"\n⚠️  Warnings/errors during listing:")
            for line in error_lines[:5]:
                print(f"   {line}")
            if len(error_lines) > 5:
                print(f"   ... and {len(error_lines) - 5} more warnings")

        if not file_entries:
            print(f"\n❌ No files found or failed to retrieve file list")
            return None

        # Sort by file size (smallest first)
        file_entries.sort(key=lambda x: x[1])

        # Write sorted entries to a SEPARATE local file
        with open('folder_detailes_sorted.txt', 'w', encoding='utf-8') as f:
            for entry, _ in file_entries:
                f.write(entry + '\n')

        # Copy local files to Google Drive
        import shutil
        shutil.copy2('folder_detailes.txt', os.path.join(OUTPUT_DIR, 'folder_detailes.txt'))
        shutil.copy2('folder_detailes_sorted.txt', os.path.join(OUTPUT_DIR, 'folder_detailes_sorted.txt'))

        print("\n" + "=" * 60)
        print(f"✅ Successfully streamed {file_count} files in real-time!")
        print("✅ Local files saved to /content/:")
        print("   • folder_detailes.txt (instant updates)")
        print("   • folder_detailes_sorted.txt (sorted by size)")
        print(f"✅ Copied to {OUTPUT_DIR}")

        return file_entries

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """
    Main function with file listing and command generation
    """
    print("🚀 Rclone File Lister & Command Generator (Subfolder Support)")
    print("=" * 65)
    print(f"📁 Config File: {RCLONE_CONFIG_PATH}")
    print(f"☁️  Source Remote: {REMOTE_NAME}")
    print(f"🎯 Destination Remote: {DESTINATION_REMOTE}")
    print("=" * 65)

    # Check if config file exists
    if not os.path.exists(RCLONE_CONFIG_PATH):
        print(f"⚠️  Warning: Config file not found at {RCLONE_CONFIG_PATH}")
        print("Please ensure the config file exists at the specified path.")
        return

    print(f"\n🌐 Step 1: Listing files from: {REMOTE_NAME}")
    print("=" * 50)

    # Get file list
    files = list_files_with_rclone_realtime(REMOTE_NAME)

    if files:
        print(f"\n🎉 File listing completed successfully!")
        print(f"📊 Total files found: {len(files)}")

        # Generate rclone commands
        print(f"\n🌐 Step 2: Generating rclone copy commands")
        print("=" * 50)

        commands = generate_rclone_commands(files)

        if commands:
            # Save commands to files
            save_commands_to_file(commands)

            print(f"\n🎉 PROCESS COMPLETED SUCCESSFULLY!")
            print(f"📈 Files ordered from smallest to largest")
            print(f"💾 File listing saved to: folder_detailes.txt")
            print(f"🔧 Commands ready for execution")

            # Show sample commands
            print(f"\n📋 Sample Commands (first 2):")
            for i, (cmd, size_mb, file_path) in enumerate(commands[:2]):
                print(f"  {i+1}. {file_path} ({size_mb} MB)")
                print(f"     {cmd}")
                print()
        else:
            print("❌ Failed to generate commands")
    else:
        print("\n❌ Failed to retrieve file list")
        print("\n🔧 Troubleshooting:")
        print(f"1. Verify config file exists: {RCLONE_CONFIG_PATH}")
        print(f"2. Check remote and folder exist: {REMOTE_NAME}")
        print("3. Test connection manually: rclone --config /content/rclone20.conf ls onedrive:")

if __name__ == "__main__":
    main()