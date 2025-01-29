import sqlite3
import shutil
import os
from datetime import datetime
import logging
from pathlib import Path

def setup_logging():
    """Configure logging to both file and console"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('database_backup.log'),
            logging.StreamHandler()
        ]
    )

def create_backup(db_path, backup_dir='backups', max_backups=10):
    """
    Create a backup of the SQLite database
    
    Args:
        db_path (str): Path to the source database file
        backup_dir (str): Directory to store backups
        max_backups (int): Maximum number of backup files to keep
    """
    try:
        # Create backup directory if it doesn't exist
        Path(backup_dir).mkdir(exist_ok=True)
        
        # Generate backup filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        db_name = Path(db_path).stem
        backup_path = Path(backup_dir) / f"{db_name}_backup_{timestamp}.db"
        
        # Ensure source database exists
        if not Path(db_path).exists():
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        # Create backup using SQLite's backup API
        with sqlite3.connect(db_path) as source:
            with sqlite3.connect(str(backup_path)) as target:
                source.backup(target)
        
        logging.info(f"Backup created successfully: {backup_path}")
        
        # Clean up old backups if exceeding max_backups
        cleanup_old_backups(backup_dir, max_backups)
        
        return True
        
    except Exception as e:
        logging.error(f"Backup failed: {str(e)}")
        return False

def cleanup_old_backups(backup_dir, max_backups):
    """Remove oldest backups if exceeding max_backups limit"""
    backups = sorted(
        Path(backup_dir).glob('*_backup_*.db'),
        key=os.path.getctime
    )
    
    while len(backups) > max_backups:
        oldest_backup = backups.pop(0)
        oldest_backup.unlink()
        logging.info(f"Removed old backup: {oldest_backup}")

if __name__ == '__main__':
    # Example usage
    setup_logging()
    db_path = 'your_database.db'
    create_backup(db_path)