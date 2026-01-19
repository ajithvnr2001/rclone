# Run this in a Colab cell to execute rclone commands from the file
# Uses get_ipython().system() - same as typing !command in a cell

# ============ CONFIGURATION ============
COMMANDS_FILE = '/content/drive/MyDrive/onedrive_migration/rclone_commands_largest_first.txt'
START_FROM = 1  # Start from command number (1-indexed, use this to resume)
# =======================================

def run_rclone_commands(commands_file, start_from=1):
    """Execute rclone commands from file using Colab's native ! execution"""
    
    # Count total commands
    with open(commands_file, 'r', encoding='utf-8') as f:
        total = sum(1 for line in f if line.strip().startswith('!rclone'))
    
    print(f"📁 Total commands: {total}")
    print(f"▶️  Starting from: {start_from}")
    print("=" * 50)
    
    completed = 0
    current = 0
    
    with open(commands_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            # Only process rclone commands
            if line.startswith('!rclone'):
                current += 1
                
                # Skip until we reach start_from
                if current < start_from:
                    continue
                
                # Remove the leading '!' for get_ipython().system()
                cmd = line[1:]
                
                try:
                    print(f"\n[{current}/{total}] Running...")
                    # This is exactly the same as typing !command in a cell
                    get_ipython().system(cmd)
                    completed += 1
                    print(f"✅ Done [{completed} completed]")
                        
                except KeyboardInterrupt:
                    print(f"\n\n⏸️  Paused at command {current}")
                    print(f"   To resume, set START_FROM = {current}")
                    break
                except Exception as e:
                    print(f"❌ Error: {e}")
    
    print("\n" + "=" * 50)
    print(f"📊 Summary: {completed} completed out of {total}")

# Run it!
run_rclone_commands(COMMANDS_FILE, START_FROM)
