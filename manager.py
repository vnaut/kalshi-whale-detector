import subprocess
import time
import logging
from datetime import datetime

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MANAGER - %(message)s')

REDIS_CONTAINER = "my-whale-redis"
MIN_REDIS_KEYS = 100  # If keys drop below this, assume DB crashed/emptied
CHECK_INTERVAL = 60   # Check every 60 seconds

def check_redis_health():
    """Returns True if Redis is healthy and populated, False if empty/crashed."""
    try:
        # Run docker exec to check DBSIZE
        result = subprocess.run(
            ["docker", "exec", REDIS_CONTAINER, "redis-cli", "DBSIZE"],
            capture_output=True, text=True
        )
        
        if result.returncode != 0:
            logging.error(f"Redis check failed: {result.stderr}")
            return False
            
        # Output is usually "(integer) 12345". We parse the number.
        output = result.stdout.strip()
        if "(integer)" in output:
            count = int(output.split()[-1])
            logging.info(f"Redis Health Check: {count} keys.")
            return count > MIN_REDIS_KEYS
        else:
            # Sometimes it just returns the number
            return int(output) > MIN_REDIS_KEYS
            
    except Exception as e:
        logging.error(f"Error checking Redis: {e}")
        return False

def stop_services():
    """Kills the tmux sessions."""
    logging.info("Stopping services...")
    sessions = ["ingestor", "analyzer", "alerter"]
    for session in sessions:
        subprocess.run(["tmux", "kill-session", "-t", session], stderr=subprocess.DEVNULL)
    time.sleep(2) # Give them a moment to die

def run_training():
    """Runs the training script and waits for it to finish."""
    logging.info("Starting Emergency Retraining...")
    # We use the venv python to run the trainer
    cmd = "/root/venv/bin/python3 train_model.py"
    
    process = subprocess.run(cmd, shell=True)
    
    if process.returncode == 0:
        logging.info("Training finished successfully.")
        return True
    else:
        logging.error("Training FAILED. Will try again next loop.")
        return False

def start_services():
    """Respawns the tmux sessions."""
    logging.info("Restarting services in tmux...")
    
    # 1. Alerter
    subprocess.run("tmux new -d -s alerter 'source /root/venv/bin/activate && python3 alerter.py'", shell=True)
    
    # 2. Analyzer
    subprocess.run("tmux new -d -s analyzer 'source /root/venv/bin/activate && python3 analyzer.py'", shell=True)
    
    # 3. Ingestor
    subprocess.run("tmux new -d -s ingestor 'source /root/venv/bin/activate && python3 ingestor.py'", shell=True)
    
    logging.info("All services restarted.")

def main():
    logging.info("--- Whale Manager Started ---")
    
    while True:
        is_healthy = check_redis_health()
        
        if not is_healthy:
            logging.warning("!!! EMERGENCY: REDIS IS EMPTY OR DOWN !!!")
            
            # 1. Stop the bots so they don't crash
            stop_services()
            
            # 2. Refill the DB
            success = run_training()
            
            # 3. If training worked, restart the bots
            if success:
                start_services()
                logging.info("System recovered. Monitoring will resume.")
            else:
                logging.error("Recovery failed. Waiting 60s before retrying...")
        
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()