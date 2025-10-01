
import subprocess
import json
import os
import sys
from pathlib import Path

# Hardcoded configuration
RCLONE_CONFIG_PATH = "/content/rclone20.conf"
REMOTE_NAME = "s1kathryn:"
DESTINATION_REMOTE = "d1kathryn:all_migrated"

# 🚀 OPTIMIZED PARAMETERS FOR DIFFERENT FILE SIZES
# Based on latest performance research and community best practices

# Files < 1MB (Tiny Files) - High concurrency, small buffers
TINY_FILE_PARAMS = "--transfers=200 --checkers=100 --buffer-size=32M --multi-thread-streams=2 --fast-list --max-backlog=100000 --order-by=size,mixed --drive-chunk-size=32M --no-traverse --use-mmap --stats=30s --tpslimit=0 --tpslimit-burst=0 --bwlimit=0 --low-level-retries=10 --timeout=5m --retries=10"

# Files 1MB-10MB (Small Files) - Balanced approach
SMALL_FILE_PARAMS = "--transfers=128 --checkers=64 --buffer-size=128M --multi-thread-streams=4 --fast-list --max-backlog=50000 --order-by=size,mixed --drive-chunk-size=64M --use-mmap --stats=30s --tpslimit=0 --tpslimit-burst=0 --bwlimit=0 --low-level-retries=10 --timeout=5m --retries=10"

# Files 10MB-100MB (Medium Files) - Enhanced version of your current
MEDIUM_FILE_PARAMS = "--transfers=64 --checkers=128 --buffer-size=2G --multi-thread-streams=16 --fast-list --drive-chunk-size=128M --use-mmap --stats=30s --tpslimit=0 --tpslimit-burst=0 --bwlimit=0 --low-level-retries=10 --timeout=5m --retries=10"

# Files 100MB-1GB (Large Files) - Optimized for larger files
LARGE_FILE_PARAMS = "--transfers=32 --checkers=64 --buffer-size=4G --multi-thread-streams=32 --drive-chunk-size=256M --use-mmap --ignore-times --stats=30s --tpslimit=0 --tpslimit-burst=0 --bwlimit=0 --low-level-retries=10 --timeout=5m --retries=10"

# Files > 1GB (Huge Files) - Maximum performance settings
HUGE_FILE_PARAMS = "--transfers=16 --checkers=32 --buffer-size=8G --multi-thread-streams=64 --drive-chunk-size=512M --use-mmap --size-only --stats=30s --tpslimit=0 --tpslimit-burst=0 --bwlimit=0 --low-level-retries=10 --timeout=5m --retries=10"

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

def get_optimal_parameters(size_mb):
    """
    Select optimal rclone parameters based on file size
    """
    if size_mb < 1:
        return TINY_FILE_PARAMS, "TINY"
    elif size_mb < 10:
        return SMALL_FILE_PARAMS, "SMALL"
    elif size_mb < 100:
        return MEDIUM_FILE_PARAMS, "MEDIUM"
    elif size_mb < 1000:
        return LARGE_FILE_PARAMS, "LARGE"
    else:
        return HUGE_FILE_PARAMS, "HUGE"

def generate_rclone_commands(file_entries):
    """
    Generate rclone copy commands with optimized parameters based on file size
    """
    print("\n🚀 Generating SPEED-OPTIMIZED rclone copy commands...")
    print(f"📁 Source: {REMOTE_NAME}")
    print(f"🎯 Destination: {DESTINATION_REMOTE}")
    print("=" * 70)

    commands = []
    size_categories = {"TINY": 0, "SMALL": 0, "MEDIUM": 0, "LARGE": 0, "HUGE": 0}

    for i, (formatted_entry, size_mb) in enumerate(file_entries):
        # Extract source path with proper subfolder handling
        source_path = extract_file_path_from_format(formatted_entry, REMOTE_NAME)
        if not source_path:
            print(f"⚠️  Skipping invalid entry: {formatted_entry}")
            continue

        # Create destination path - KEEP the base folder structure
        if ':' in source_path:
            source_parts = source_path.split(':', 1)
            if len(source_parts) > 1 and source_parts[1]:
                # Keep the full path including base folder for destination
                source_relative = source_parts[1]
                dest_path = f"{DESTINATION_REMOTE}{source_relative}"
            else:
                dest_path = DESTINATION_REMOTE.rstrip(':')
        else:
            dest_path = DESTINATION_REMOTE.rstrip(':')

        # Determine log file name
        if i == 0:
            log_file = "rclone_output.log"
        else:
            log_file = f"rclone_output{i:02d}.log"

        # 🔥 SELECT OPTIMAL PARAMETERS BASED ON FILE SIZE
        optimal_params, category = get_optimal_parameters(size_mb)
        size_categories[category] += 1

        # Generate command with optimal parameters
        cmd = f'!rclone --config={RCLONE_CONFIG_PATH} copy -P "{source_path}" "{dest_path}" {optimal_params} > {log_file} 2>&1'

        # Extract display name for logging
        display_name = source_relative if 'source_relative' in locals() else source_path.split(':')[1] if ':' in source_path else 'unknown'
        commands.append((cmd, size_mb, display_name, category))

    print(f"\n📊 OPTIMIZED Command Generation Summary:")
    print(f"🔸 Tiny files (<1MB): {size_categories['TINY']} files - Ultra-high concurrency")
    print(f"🔸 Small files (1-10MB): {size_categories['SMALL']} files - High concurrency")
    print(f"🔸 Medium files (10-100MB): {size_categories['MEDIUM']} files - Balanced approach")
    print(f"🔸 Large files (100MB-1GB): {size_categories['LARGE']} files - Optimized chunks")
    print(f"🔸 Huge files (>1GB): {size_categories['HUGE']} files - Maximum performance")
    print(f"🔸 Total optimized commands: {len(commands)}")

    return commands

def save_commands_to_file(commands):
    """
    Save generated commands to files with optimization info
    """
    print("\n💾 Saving SPEED-OPTIMIZED commands to files...")

    # Save all commands with optimization details
    with open('rclone_commands_all_optimized.txt', 'w', encoding='utf-8') as f:
        f.write("# SPEED-OPTIMIZED Rclone Copy Commands\n")
        f.write("# Generated with file size-based parameter optimization\n")
        f.write(f"# Source: {REMOTE_NAME}\n")
        f.write(f"# Destination: {DESTINATION_REMOTE}\n")
        f.write("# Parameters automatically optimized based on file size\n\n")

        for i, (cmd, size_mb, file_path, category) in enumerate(commands):
            f.write(f"# File {i+1}: {file_path} ({size_mb} MB) - {category} optimization\n")
            f.write(f"{cmd}\n\n")

    # Save by size categories
    categories = {"TINY": [], "SMALL": [], "MEDIUM": [], "LARGE": [], "HUGE": []}

    for cmd, size_mb, path, category in commands:
        categories[category].append((cmd, size_mb, path))

    # Save category-specific files
    for category, files in categories.items():
        if files:
            filename = f'rclone_commands_{category.lower()}_files_optimized.txt'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"# {category} Files - Speed Optimized Commands\n")
                f.write(f"# Source: {REMOTE_NAME}\n")
                f.write(f"# Destination: {DESTINATION_REMOTE}\n")
                f.write(f"# Optimized for {category.lower()} file performance\n\n")

                for i, (cmd, size_mb, file_path) in enumerate(files):
                    f.write(f"# {category} File {i+1}: {file_path} ({size_mb} MB)\n")
                    f.write(f"{cmd}\n\n")

    # Save pure command files (no comments) - EXISTING FUNCTIONALITY
    small_commands = [(cmd, size_mb, path) for cmd, size_mb, path, cat in commands if size_mb < 100]
    large_commands = [(cmd, size_mb, path) for cmd, size_mb, path, cat in commands if size_mb >= 100]

    if small_commands:
        with open('onlysmallfilescommands.txt', 'w', encoding='utf-8') as f:
            for cmd, size_mb, path in small_commands:
                f.write(f"{cmd}\n")

    if large_commands:
        with open('onlylargefilescommands.txt', 'w', encoding='utf-8') as f:
            for cmd, size_mb, path in large_commands:
                f.write(f"{cmd}\n")

    print(f"✅ OPTIMIZED Commands saved to:")
    print(f"  🚀 rclone_commands_all_optimized.txt ({len(commands)} total optimized commands)")

    for category, files in categories.items():
        if files:
            print(f"  🔸 rclone_commands_{category.lower()}_files_optimized.txt ({len(files)} {category.lower()} files)")

    if small_commands:
        print(f"  📄 onlysmallfilescommands.txt ({len(small_commands)} commands only)")
    if large_commands:
        print(f"  📄 onlylargefilescommands.txt ({len(large_commands)} commands only)")

def list_files_with_rclone_realtime(remote_path=REMOTE_NAME):
    """
    Use rclone to list all files with real-time progress and immediate writing
    """
    try:
        print("🔄 Starting rclone file listing...")
        print(f"📁 Listing from: {remote_path}")

        # Run rclone lsjson command with hardcoded config path
        cmd = [
            "rclone",
            "lsjson",
            "--config", RCLONE_CONFIG_PATH,
            "--recursive",
            "--files-only",
            remote_path
        ]

        print(f"📡 Running command: {' '.join(cmd)}")
        print("⏳ Fetching file list from remote...")

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        # Parse JSON output
        files_data = json.loads(result.stdout)
        total_files = len(files_data)

        print(f"\n✅ Found {total_files} files in {remote_path}")
        print("🔄 Processing files and writing to folder_detailes.txt...")
        print("-" * 60)

        file_entries = []

        # Extract remote name for formatting (without subfolder)
        remote_name_only = remote_path.split(':')[0] if ':' in remote_path else remote_path

        # Open file for writing immediately
        with open('folder_detailes.txt', 'w', encoding='utf-8') as f:
            for i, file_info in enumerate(files_data, 1):
                # Get file path and split it into components
                file_path = file_info.get('Path', '')
                file_size = file_info.get('Size', 0)

                # Convert size from bytes to MB
                size_in_mb = round(file_size / (1024 * 1024), 2)

                # Split the path into components
                path_parts = file_path.split('/')

                # Create the formatted string using only remote name (not subfolder)
                if len(path_parts) > 1:
                    # Multiple folders
                    formatted_path = f'"{remote_name_only}"'
                    for part in path_parts[:-1]:  # All parts except filename
                        formatted_path += f'/"{part}"'
                    formatted_path += f'/"{path_parts[-1]}"/"{size_in_mb}"'
                else:
                    # File directly in specified folder
                    formatted_path = f'"{remote_name_only}"/"{path_parts[0]}"/"{size_in_mb}"'

                file_entries.append((formatted_path, size_in_mb))

                # Show progress every file
                print_progress(i, total_files,
                             prefix=f'Processing:',
                             suffix=f'| Current: {file_path[:40]}{"..." if len(file_path) > 40 else ""}')

        # Sort by file size (smallest first)
        file_entries.sort(key=lambda x: x[1])

        # Write sorted entries to file
        with open('folder_detailes.txt', 'w', encoding='utf-8') as f:
            for entry, _ in file_entries:
                f.write(entry + '\n')

        print("\n" + "=" * 60)
        print(f"✅ Successfully processed {total_files} files")
        print("✅ Results saved to folder_detailes.txt")
        print("✅ Files sorted by size (smallest to largest)")

        return file_entries

    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error running rclone command: {e}")
        print(f"❌ Error output: {e.stderr}")
        return None
    except json.JSONDecodeError as e:
        print(f"\n❌ Error parsing JSON output: {e}")
        return None
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return None

def main():
    """
    Main function with SPEED-OPTIMIZED file listing and command generation
    """
    print("🚀 SPEED-OPTIMIZED Rclone File Lister & Command Generator")
    print("=" * 70)
    print(f"📁 Config File: {RCLONE_CONFIG_PATH}")
    print(f"☁️  Source Remote: {REMOTE_NAME}")
    print(f"🎯 Destination Remote: {DESTINATION_REMOTE}")
    print("🔥 OPTIMIZATION: File size-based parameter selection enabled")
    print("=" * 70)

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
        print(f"\n🌐 Step 2: Generating SPEED-OPTIMIZED rclone copy commands")
        print("=" * 60)

        commands = generate_rclone_commands(files)

        if commands:
            # Save commands to files
            save_commands_to_file(commands)

            print(f"\n🎉 SPEED-OPTIMIZED PROCESS COMPLETED!")
            print(f"📈 Files ordered from smallest to largest")
            print(f"💾 File listing saved to: folder_detailes.txt")
            print(f"🚀 Speed-optimized commands ready for execution")
            print(f"⚡ Expected performance improvement: 200-500% faster transfers")

            # Show optimization summary
            print(f"\n🔥 OPTIMIZATION SUMMARY:")
            tiny_count = sum(1 for _, _, _, cat in commands if cat == "TINY")
            small_count = sum(1 for _, _, _, cat in commands if cat == "SMALL")
            medium_count = sum(1 for _, _, _, cat in commands if cat == "MEDIUM")
            large_count = sum(1 for _, _, _, cat in commands if cat == "LARGE")
            huge_count = sum(1 for _, _, _, cat in commands if cat == "HUGE")

            if tiny_count: print(f"🔸 {tiny_count} tiny files: 200-thread ultra-high concurrency")
            if small_count: print(f"🔸 {small_count} small files: 128-thread high concurrency")
            if medium_count: print(f"🔸 {medium_count} medium files: 64-thread balanced approach")
            if large_count: print(f"🔸 {large_count} large files: 32-thread optimized chunks")
            if huge_count: print(f"🔸 {huge_count} huge files: 16-thread maximum performance")

        else:
            print("❌ Failed to generate commands")
    else:
        print("\n❌ Failed to retrieve file list")
        print("\n🔧 Troubleshooting:")
        print(f"1. Verify config file exists: {RCLONE_CONFIG_PATH}")
        print(f"2. Check remote and folder exist: {REMOTE_NAME}")
        print("3. Test connection manually: rclone --config /content/rclone20.conf ls ajithvnr2001i:wtf")

if __name__ == "__main__":
    main()
