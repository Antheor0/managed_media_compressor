import smtplib
import datetime
import logging
import os
import shutil
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, Optional

logger = logging.getLogger('MediaCompressor.NotificationService')

class NotificationService:
    """Service for sending notifications about compression status."""
    
    def __init__(self, config: Dict[str, Any], db_logger=None):
        """Initialize the notification service with configuration."""
        self.config = config
        self.db_logger = db_logger  # For database event logging
    
    def send_notification(self, message: str, level: str = "info"):
        """
        Send notifications through various channels.
        
        Args:
            message: The notification message
            level: The notification level ("info", "warning", "error")
        """
        # Email notifications
        if (self.config["notifications"]["email"]["enabled"] and 
            ((level == "error" and self.config["notifications"]["email"]["on_error"]) or 
             (level == "info" and self.config["notifications"]["email"]["on_completion"]))):
            self._send_email(
                subject=f"Media Compressor {level.capitalize()}", 
                body=message
            )
        
        # Webhook notifications
        if (self.config["notifications"]["webhook"]["enabled"] and 
            ((level == "error" and self.config["notifications"]["webhook"]["on_error"]) or 
             (level == "info" and self.config["notifications"]["webhook"]["on_completion"]))):
            self._send_webhook({
                "level": level,
                "message": message,
                "timestamp": datetime.datetime.now().isoformat()
            })
        
        # Log to system events if database logger is available
        if self.db_logger:
            self.db_logger(
                f"notification_{level}",
                message,
                level
            )
    
    def _send_email(self, subject: str, body: str):
        """
        Send an email notification.
        
        Args:
            subject: Email subject
            body: Email body
        """
        try:
            config = self.config["notifications"]["email"]
            
            msg = MIMEMultipart()
            msg['From'] = config["from_addr"]
            msg['To'] = config["to_addr"]
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(config["smtp_server"], config["smtp_port"])
            server.starttls()
            server.login(config["username"], config["password"])
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email notification sent: {subject}")
        except Exception as e:
            logger.error(f"Error sending email notification: {str(e)}")
    
    def _send_webhook(self, data: Dict[str, Any]):
        """
        Send a webhook notification.
        
        Args:
            data: Dictionary containing notification data
        """
        try:
            webhook_url = self.config["notifications"]["webhook"]["url"]
            
            # Add additional system info
            data["system_info"] = {
                "hostname": os.uname().nodename,
                "free_space_gb": shutil.disk_usage(self.config["temp_dir"]).free / (1024**3)
            }
            
            response = requests.post(
                webhook_url,
                json=data,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code < 200 or response.status_code >= 300:
                logger.warning(f"Webhook response error: {response.status_code} - {response.text}")
            else:
                logger.info(f"Webhook notification sent successfully")
        
        except Exception as e:
            logger.error(f"Error sending webhook notification: {str(e)}")
    
    def send_completion_notification(self, stats: Dict[str, Any]):
        """
        Send a notification for completion of compression session.
        
        Args:
            stats: Dictionary with compression statistics
        """
        if not (self.config["notifications"]["email"]["on_completion"] or 
                self.config["notifications"]["webhook"]["on_completion"]):
            return
            
        files_processed = stats.get("files_processed", 0)
        errors = stats.get("errors", 0)
        total_original_size = stats.get("total_original_size", 0)
        total_compressed_size = stats.get("total_compressed_size", 0)
        
        # Calculate savings
        if total_original_size > 0:
            savings_gb = (total_original_size - total_compressed_size) / (1024**3)
            savings_percentage = (1 - (total_compressed_size / total_original_size)) * 100
            savings_text = f"{savings_gb:.2f}GB ({savings_percentage:.2f}%)"
        else:
            savings_text = "0 GB (0%)"
        
        self.send_notification(
            f"Compression session completed. "
            f"Files processed: {files_processed}, "
            f"Errors: {errors}, "
            f"Space saved: {savings_text}",
            level="info"
        )