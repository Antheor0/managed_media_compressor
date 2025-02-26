import json
import base64
import threading
import datetime
import os
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import List, Dict, Tuple, Optional, Set, Any, Callable
from media_scanner import MediaScanner
from media_compressor import MediaCompressor
from media_database import MediaDatabase
from config_validator import ConfigValidator

logger = logging.getLogger('MediaCompressor.WebServer')

class MediaCompressionWebServer:
    """
    Web server for monitoring the media compression system.
    Provides both HTML UI and JSON API endpoints with optional authentication.
    """
    def __init__(self, config: Dict, db: MediaDatabase, scanner: MediaScanner, compressor: MediaCompressor):
        self.config = config
        self.db = db
        self.scanner = scanner
        self.compressor = compressor
        self.server = None
        self.active_scanners = []  # Track concurrent scanners
    
    def start(self):
        """Start the web server in a background thread."""
        if not self.config["web_interface"]["enabled"]:
            return
        
        host = self.config["web_interface"]["host"]
        port = self.config["web_interface"]["port"]
        
        class AuthHandler(BaseHTTPRequestHandler):
            """Handler for the HTTP requests with authentication."""
            
            def do_AUTHHEAD(self):
                """Send authentication headers."""
                self.send_response(401)
                self.send_header('WWW-Authenticate', 'Basic realm="Media Compressor"')
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(b'Authentication required')
            
            def check_auth(self):
                """Check if the request has valid authentication."""
                # Access web_server through self.server
                if not self.server.web_server.config["web_interface"]["secure"]:
                    return True
                        
                auth_header = self.headers.get('Authorization')
                if not auth_header:
                    self.do_AUTHHEAD()
                    return False
                
                try:
                    auth_decoded = base64.b64decode(auth_header.split(' ')[1]).decode('utf-8')
                    username, password = auth_decoded.split(':')
                    
                    # Access web_server through self.server
                    valid_username = self.server.web_server.config["web_interface"]["username"]
                    valid_password = self.server.web_server.config["web_interface"]["password"]
                    
                    if username == valid_username and password == valid_password:
                        return True
                    else:
                        self.do_AUTHHEAD()
                        return False
                except Exception:
                    self.do_AUTHHEAD()
                    return False
            
            def do_GET(self):
                """Handle GET requests with authentication."""
                if not self.check_auth():
                    return
                
                if self.path == '/' or self.path == '/index.html':
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    
                    # Get system status
                    db_stats = self.server.web_server.db.get_statistics()
                    scanner_status = self.server.web_server.scanner.get_scan_status()
                    compressor_status = self.server.web_server.compressor.get_compression_status()
                    
                    # Get recent events
                    recent_events = self.server.web_server.db.get_recent_events(20)
                    
                    # Generate HTML content for the dashboard
                    html = self.generate_dashboard_html(db_stats, scanner_status, compressor_status, recent_events)
                    self.wfile.write(html.encode())
                
                elif self.path == '/api/stats':
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    
                    # Collect all stats
                    stats = {
                        "database": self.server.web_server.db.get_statistics(),
                        "scanner": self.server.web_server.scanner.get_scan_status(),
                        "compressor": self.server.web_server.compressor.get_compression_status(),
                        "timestamp": datetime.datetime.now().isoformat()
                    }
                    
                    self.wfile.write(json.dumps(stats).encode())
                
                elif self.path == '/api/events':
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    
                    # Get recent events
                    events = self.server.web_server.db.get_recent_events(50)
                    self.wfile.write(json.dumps(events).encode())
                
                elif self.path.startswith('/control/'):
                    # Handle control commands
                    command = self.path.split('/')[-1]
                    result = self.handle_control_command(command)
                    
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(result).encode())
                
                else:
                    self.send_response(404)
                    self.end_headers()
                    self.wfile.write(b'Not Found')
            
            def handle_control_command(self, command):
                """Handle control commands from the dashboard."""
                if command == 'pause':
                    self.server.web_server.compressor.pause_compression()
                    return {"status": "success", "message": "Compression paused"}
                    
                elif command == 'resume':
                    self.server.web_server.compressor.resume_compression()
                    return {"status": "success", "message": "Compression resumed"}
                    
                elif command == 'stop':
                    self.server.web_server.compressor.stop_compression()
                    return {"status": "success", "message": "Compression stopped"}
                    
                elif command == 'start_scan':
                    # Start a scan in a new thread
                    scan_thread = threading.Thread(
                        target=self.server.web_server.scanner.run_scan,
                        daemon=True
                    )
                    scan_thread.start()
                    return {"status": "success", "message": "Scan started"}
                    
                elif command == 'start_compression':
                    # Start compression in a new thread
                    comp_thread = threading.Thread(
                        target=self.server.web_server.compressor.process_compression_queue,
                        kwargs={"force_now": True},
                        daemon=True
                    )
                    comp_thread.start()
                    return {"status": "success", "message": "Compression started"}
                    
                elif command == 'reload_config':
                    # Reload configuration
                    result = self.server.web_server.reload_configuration()
                    if result["success"]:
                        return {"status": "success", "message": "Configuration reloaded"}
                    else:
                        return {"status": "error", "message": result["message"]}
                
                else:
                    return {"status": "error", "message": f"Unknown command: {command}"}
            
            def generate_dashboard_html(self, db_stats, scanner_status, compressor_status, events):
                """Generate HTML for the dashboard."""
                # Get status counts with safe defaults
                status_counts = db_stats.get('status_counts', {})
                
                # Use the actual total from the database instead of calculating it
                total_files = db_stats.get('total_files', 0)
                
                # Get individual status counts safely
                pending_files = status_counts.get('pending', 0)
                completed_files = status_counts.get('completed', 0)
                skipped_files = status_counts.get('skipped', 0)
                error_files = status_counts.get('error', 0)
                
                # Get space saved with safe defaults
                space_saved = db_stats.get('space_saved', 0)
                space_saved_gb = space_saved / (1024 * 1024 * 1024) if space_saved else 0
                
                # Scanner status
                scanner_status_text = scanner_status.get('status', 'unknown').upper()
                scanner_badge_class = "bg-primary" if scanner_status.get('status') == 'scanning' else "bg-secondary"
                
                # Get scan count when scanning
                files_scanned = scanner_status.get('files_scanned', 0)
                
                # Compressor status
                compressor_status_text = compressor_status.get('status', 'unknown').upper()
                compressor_badge_class = "bg-success" if compressor_status.get('status') == 'compressing' else "bg-secondary"
                if compressor_status.get('paused', False):
                    compressor_badge_class = "bg-warning"
                    compressor_status_text = "PAUSED"
                
                # Extract active job info
                active_jobs = compressor_status.get('active_jobs', [])
                
                # The HTML template construction
                html = f"""<!DOCTYPE html>
                <html>
                <head>
                    <title>Media Compressor Dashboard</title>
                    <meta http-equiv="refresh" content="10">
                    <meta name="viewport" content="width=device-width, initial-scale=1">
                    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
                    <style>
                        body {{ padding: 20px; background-color: #f8f9fa; }}
                        .dashboard-container {{ max-width: 1400px; margin: 0 auto; }}
                        .card {{ margin-bottom: 20px; box-shadow: 0 0.125rem 0.25rem rgba(0,0,0,0.075); }}
                        .card-header {{ background-color: #f1f8ff; }}
                        .status-badge {{ font-size: 85%; }}
                        .progress {{ height: 20px; }}
                        .progress-bar {{ font-size: 0.8rem; line-height: 20px; }}
                        .log-container {{ height: 350px; overflow: auto; background-color: #212529; color: #f8f9fa; padding: 10px; border-radius: 4px; font-family: monospace; font-size: 0.9rem; }}
                        .stats-table {{ font-size: 0.9rem; }}
                        .stats-table th {{ width: 50%; }}
                        .compression-job {{ border-left: 4px solid #0d6efd; padding-left: 10px; margin-bottom: 10px; }}
                        .scanning {{ background-color: #e8f4f8; }}
                        .file-path {{ font-family: monospace; font-size: 0.85rem; color: #495057; }}
                        .file-status {{ font-weight: bold; }}
                        .summary-value {{ font-size: 1.2rem; font-weight: bold; }}
                        .control-buttons {{ margin-bottom: 20px; }}
                        .event-list {{ height: 200px; overflow-y: auto; }}
                        .event-item {{ border-left: 3px solid; padding-left: 10px; margin-bottom: 8px; }}
                        .event-error {{ border-color: #dc3545; }}
                        .event-warning {{ border-color: #ffc107; }}
                        .event-info {{ border-color: #0dcaf0; }}
                        .logs-container {{ height: 250px; overflow-y: auto; background-color: #212529; color: #f8f9fa; 
                                          font-family: monospace; font-size: 0.9rem; padding: 10px; border-radius: 4px; }}
                        .log-entry {{ margin-bottom: 4px; border-bottom: 1px solid #444; padding-bottom: 4px; }}
                        .info-box {{ padding: 10px; border-radius: 5px; margin-bottom: 10px; }}
                        .status-indicator {{ width: 12px; height: 12px; border-radius: 50%; display: inline-block; margin-right: 5px; }}
                        .status-active {{ background-color: #28a745; }}
                        .status-inactive {{ background-color: #dc3545; }}
                        .status-pending {{ background-color: #ffc107; }}
                        .status-row {{ display: flex; justify-content: space-between; margin-bottom: 5px; }}
                        .status-label {{ font-weight: bold; }}
                        .status-value {{ text-align: right; }}
                        .metric-circle {{ width: 120px; height: 120px; margin: 0 auto; display: flex; 
                                         flex-direction: column; justify-content: center; align-items: center;
                                         border-radius: 50%; border: 6px solid #e9ecef; }}
                        .metric-value {{ font-size: 1.5rem; font-weight: bold; margin-bottom: 0; line-height: 1; }}
                        .metric-label {{ font-size: 0.8rem; color: #6c757d; }}
                        .text-xs {{ font-size: 0.75rem; }}
                        .text-sm {{ font-size: 0.875rem; }}
                    </style>
                </head>
                <body>
                    <div class="dashboard-container">
                        <div class="d-flex justify-content-between align-items-center mb-4">
                            <h1 class="mb-0">Media Compressor Dashboard</h1>
                            <div>
                                <span class="badge bg-secondary">Last Updated: {datetime.datetime.now().strftime('%H:%M:%S')}</span>
                            </div>
                        </div>
                        
                        <!-- Control Buttons -->
                        <div class="control-buttons d-flex gap-2 mb-4">
                            <div class="card flex-grow-1">
                                <div class="card-header">
                                    <h5 class="mb-0">Compression Controls</h5>
                                </div>
                                <div class="card-body d-flex gap-2">
                                    <button class="btn btn-primary" onclick="location.href='/control/start_compression'">Start Compression</button>
                                    <button class="btn btn-warning" onclick="location.href='/control/pause'">Pause</button>
                                    <button class="btn btn-success" onclick="location.href='/control/resume'">Resume</button>
                                    <button class="btn btn-danger" onclick="location.href='/control/stop'">Stop</button>
                                </div>
                            </div>
                            
                            <div class="card flex-grow-1">
                                <div class="card-header">
                                    <h5 class="mb-0">Scanner Controls</h5>
                                </div>
                                <div class="card-body">
                                    <button class="btn btn-primary" onclick="location.href='/control/start_scan'">Start Scan</button>
                                    <button class="btn btn-info" onclick="location.href='/control/reload_config'">Reload Config</button>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Scanner Stats Box -->
                        <div class="row mb-4">
                            <div class="col-md-6">
                                <div class="card h-100">
                                    <div class="card-header d-flex justify-content-between align-items-center">
                                        <h5 class="mb-0">Scanner Status</h5>
                                        <span class="badge {scanner_badge_class}">{scanner_status_text}</span>
                                    </div>
                                    <div class="card-body">
                                        {self.generate_scanner_stats_html(scanner_status)}
                                    </div>
                                </div>
                            </div>
                            
                            <!-- Compression Stats Box -->
                            <div class="col-md-6">
                                <div class="card h-100">
                                    <div class="card-header d-flex justify-content-between align-items-center">
                                        <h5 class="mb-0">Compression Status</h5>
                                        <span class="badge {compressor_badge_class}">{compressor_status_text}</span>
                                    </div>
                                    <div class="card-body">
                                        {self.generate_compression_stats_html(compressor_status)}
                                    </div>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Database Stats Box -->
                        <div class="row mb-4">
                            <div class="col-12">
                                <div class="card">
                                    <div class="card-header">
                                        <h5 class="mb-0">Database Statistics</h5>
                                    </div>
                                    <div class="card-body">
                                        {self.generate_database_stats_html(db_stats)}
                                    </div>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Active Jobs -->
                        <div class="row mb-4">
                            <div class="col-12">
                                <div class="card">
                                    <div class="card-header">
                                        <h5 class="mb-0">Current Compression Progress</h5>
                                    </div>
                                    <div class="card-body">
                                        {self.generate_active_jobs_html(active_jobs)}
                                    </div>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Recent System Logs -->
                        <div class="row mb-4">
                            <div class="col-12">
                                <div class="card">
                                    <div class="card-header">
                                        <h5 class="mb-0">Recent System Logs</h5>
                                    </div>
                                    <div class="card-body p-0">
                                        <div class="logs-container">
                                            {self.generate_logs_html(events)}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    
                </body>
                </html>
                """
                # Note: Removed the problematic JavaScript to fix syntax errors
                return html
                
            def generate_scanner_stats_html(self, scanner_status):
                """Generate HTML for the scanner stats box."""
                if scanner_status.get("status") == "idle":
                    return """
                    <div class="text-center py-4">
                        <div class="mb-3">
                            <i class="bi bi-pause-circle" style="font-size: 2rem;"></i>
                        </div>
                        <h5>Scanner is currently idle</h5>
                        <p class="text-muted">Use the "Start Scan" button to begin scanning.</p>
                    </div>
                    """
                
                # If scanning is active
                files_scanned = scanner_status.get('files_scanned', 0)
                new_files = scanner_status.get('new_files', 0)
                changed_files = scanner_status.get('changed_files', 0)
                progress = scanner_status.get('progress', 0)
                current_dir = scanner_status.get('current_directory', 'N/A')
                duration = scanner_status.get('duration', 0)
                
                # Format duration
                if duration < 60:
                    duration_str = f"{duration:.1f} seconds"
                elif duration < 3600:
                    minutes = int(duration // 60)
                    seconds = int(duration % 60)
                    duration_str = f"{minutes}m {seconds}s"
                else:
                    hours = int(duration // 3600)
                    minutes = int((duration % 3600) // 60)
                    duration_str = f"{hours}h {minutes}m"
                
                # Format ETA
                eta_html = ""
                if scanner_status.get('eta_seconds'):
                    eta_seconds = scanner_status['eta_seconds']
                    if eta_seconds < 60:
                        eta_str = f"{int(eta_seconds)}s"
                    elif eta_seconds < 3600:
                        minutes = int(eta_seconds // 60)
                        seconds = int(eta_seconds % 60)
                        eta_str = f"{minutes}m {seconds}s"
                    else:
                        hours = int(eta_seconds // 3600)
                        minutes = int((eta_seconds % 3600) // 60)
                        eta_str = f"{hours}h {minutes}m"
                    
                    eta_html = f"""
                    <div class="info-box bg-light">
                        <div class="d-flex justify-content-between align-items-center">
                            <span><strong>Estimated Time Remaining:</strong></span>
                            <span class="badge bg-info">{eta_str}</span>
                        </div>
                    </div>
                    """
                
                # Create scanner stats HTML
                return f"""
                <div>
                    <div class="progress mb-3">
                        <div class="progress-bar progress-bar-striped progress-bar-animated" 
                            role="progressbar" 
                            style="width: {progress}%;" 
                            aria-valuenow="{progress}" 
                            aria-valuemin="0" 
                            aria-valuemax="100">
                            {progress:.1f}%
                        </div>
                    </div>
                    
                    {eta_html}
                    
                    <div class="info-box bg-light mb-3">
                        <div class="text-muted mb-2">Currently Scanning:</div>
                        <div class="file-path">{current_dir}</div>
                    </div>
                    
                    <div class="row g-2 text-center">
                        <div class="col-md-3">
                            <div class="metric-circle bg-primary bg-opacity-10">
                                <div class="metric-value">{files_scanned}</div>
                                <div class="metric-label">Files Scanned</div>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <div class="metric-circle bg-success bg-opacity-10">
                                <div class="metric-value">{new_files}</div>
                                <div class="metric-label">New Files</div>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <div class="metric-circle bg-info bg-opacity-10">
                                <div class="metric-value">{changed_files}</div>
                                <div class="metric-label">Changed Files</div>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <div class="metric-circle bg-warning bg-opacity-10">
                                <div class="metric-value">{duration_str}</div>
                                <div class="metric-label">Duration</div>
                            </div>
                        </div>
                    </div>
                </div>
                """
                
            def generate_compression_stats_html(self, compressor_status):
                """Generate HTML for the compression stats box."""
                if compressor_status.get("status") == "idle":
                    return """
                    <div class="text-center py-4">
                        <div class="mb-3">
                            <i class="bi bi-pause-circle" style="font-size: 2rem;"></i>
                        </div>
                        <h5>Compressor is currently idle</h5>
                        <p class="text-muted">Use the "Start Compression" button to begin compressing.</p>
                    </div>
                    """
                
                # If compression is active
                files_processed = compressor_status.get('files_processed', 0)
                errors = compressor_status.get('errors', 0)
                duration = compressor_status.get('duration', 0)
                duration_formatted = compressor_status.get('duration_formatted', '0s')
                
                # Get ETA information
                eta_info = compressor_status.get('eta', {})
                eta_formatted = eta_info.get('eta_formatted', 'Unknown')
                remaining_files = eta_info.get('total_files', 0)
                avg_time = eta_info.get('average_time_per_file', 0)
                
                # Get size information
                original_size = compressor_status.get('total_original_size', 0)
                compressed_size = compressor_status.get('total_compressed_size', 0)
                
                # Calculate savings
                saved_size = original_size - compressed_size
                if original_size > 0:
                    savings_percent = (saved_size / original_size) * 100
                else:
                    savings_percent = 0
                
                # Format sizes for display
                original_gb = original_size / (1024**3)
                compressed_gb = compressed_size / (1024**3)
                saved_gb = saved_size / (1024**3)
                
                return f"""
                <div>
                    <div class="row g-2 text-center mb-3">
                        <div class="col-md-4">
                            <div class="metric-circle bg-primary bg-opacity-10">
                                <div class="metric-value">{files_processed}</div>
                                <div class="metric-label">Files Processed</div>
                            </div>
                        </div>
                        <div class="col-md-4">
                            <div class="metric-circle bg-danger bg-opacity-10">
                                <div class="metric-value">{errors}</div>
                                <div class="metric-label">Errors</div>
                            </div>
                        </div>
                        <div class="col-md-4">
                            <div class="metric-circle bg-success bg-opacity-10">
                                <div class="metric-value">{duration_formatted}</div>
                                <div class="metric-label">Running Time</div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="info-box bg-light mb-3">
                        <div class="status-row">
                            <div class="status-label">ETA:</div>
                            <div class="status-value">{eta_formatted}</div>
                        </div>
                        <div class="status-row">
                            <div class="status-label">Remaining Files:</div>
                            <div class="status-value">{remaining_files}</div>
                        </div>
                        <div class="status-row">
                            <div class="status-label">Avg. Time/File:</div>
                            <div class="status-value">{avg_time:.1f}s</div>
                        </div>
                    </div>
                    
                    <div class="info-box bg-light">
                        <div class="status-row">
                            <div class="status-label">Original Size:</div>
                            <div class="status-value">{original_gb:.2f} GB</div>
                        </div>
                        <div class="status-row">
                            <div class="status-label">Compressed Size:</div>
                            <div class="status-value">{compressed_gb:.2f} GB</div>
                        </div>
                        <div class="status-row">
                            <div class="status-label">Space Saved:</div>
                            <div class="status-value">{saved_gb:.2f} GB ({savings_percent:.1f}%)</div>
                        </div>
                    </div>
                </div>
                """
            
            def generate_database_stats_html(self, db_stats):
                """Generate HTML for the database stats box."""
                # Get status counts with safe defaults
                status_counts = db_stats.get('status_counts', {})
                
                # Parse individual statuses
                new_files = status_counts.get('new', 0)
                pending_files = status_counts.get('pending', 0)
                in_progress_files = status_counts.get('in_progress', 0)
                completed_files = status_counts.get('completed', 0)
                skipped_files = status_counts.get('skipped', 0)
                error_files = status_counts.get('error', 0)
                paused_files = status_counts.get('paused', 0)
                
                # Get total file stats
                total_files = db_stats.get('total_files', 0)
                total_original_size = db_stats.get('total_original_size', 0)
                total_compressed_size = db_stats.get('total_compressed_size', 0)
                space_saved = db_stats.get('space_saved', 0)
                savings_percentage = db_stats.get('savings_percentage', 0)
                
                # Calculate size in GB for display
                original_size_gb = total_original_size / (1024**3)
                compressed_size_gb = total_compressed_size / (1024**3)
                space_saved_gb = space_saved / (1024**3)
                
                # Get processing time stats
                processing_times = db_stats.get('processing_times', {})
                avg_time = processing_times.get('average_seconds', 0)
                min_time = processing_times.get('min_seconds', 0)
                max_time = processing_times.get('max_seconds', 0)
                
                # Format times for display
                if avg_time < 60:
                    avg_time_str = f"{avg_time:.1f} seconds"
                elif avg_time < 3600:
                    avg_time_str = f"{avg_time/60:.1f} minutes"
                else:
                    avg_time_str = f"{avg_time/3600:.1f} hours"
                
                # Get estimated remaining time
                estimated_time = db_stats.get('estimated_remaining_time', 0)
                
                if estimated_time < 60:
                    eta_str = f"{estimated_time:.1f} seconds"
                elif estimated_time < 3600:
                    eta_str = f"{estimated_time/60:.1f} minutes"
                elif estimated_time < 86400:
                    eta_str = f"{estimated_time/3600:.1f} hours"
                else:
                    eta_str = f"{estimated_time/86400:.1f} days"
                
                return f"""
                <div class="row">
                    <!-- File Status Summary -->
                    <div class="col-md-6">
                        <h6 class="mb-3">File Status Summary</h6>
                        <div class="row g-2">
                            <div class="col-md-4">
                                <div class="card bg-primary text-white">
                                    <div class="card-body p-2 text-center">
                                        <div class="h3">{total_files}</div>
                                        <div class="text-xs text-white-50">TOTAL FILES</div>
                                    </div>
                                </div>
                            </div>
                            <div class="col-md-4">
                                <div class="card bg-success text-white">
                                    <div class="card-body p-2 text-center">
                                        <div class="h3">{completed_files}</div>
                                        <div class="text-xs text-white-50">COMPLETED</div>
                                    </div>
                                </div>
                            </div>
                            <div class="col-md-4">
                                <div class="card bg-warning text-dark">
                                    <div class="card-body p-2 text-center">
                                        <div class="h3">{pending_files}</div>
                                        <div class="text-xs text-dark-50">PENDING</div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        
                        <div class="table-responsive mt-3">
                            <table class="table table-sm table-striped">
                                <thead>
                                    <tr>
                                        <th>Status</th>
                                        <th class="text-end">Count</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr>
                                        <td>New</td>
                                        <td class="text-end">{new_files}</td>
                                    </tr>
                                    <tr>
                                        <td>In Progress</td>
                                        <td class="text-end">{in_progress_files}</td>
                                    </tr>
                                    <tr>
                                        <td>Skipped</td>
                                        <td class="text-end">{skipped_files}</td>
                                    </tr>
                                    <tr>
                                        <td>Error</td>
                                        <td class="text-end">{error_files}</td>
                                    </tr>
                                    <tr>
                                        <td>Paused</td>
                                        <td class="text-end">{paused_files}</td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                    
                    <!-- Size and Time Statistics -->
                    <div class="col-md-6">
                        <h6 class="mb-3">Size and Time Statistics</h6>
                        
                        <div class="card mb-3">
                            <div class="card-header bg-light py-1">
                                <h6 class="mb-0">Storage Statistics</h6>
                            </div>
                            <div class="card-body p-3">
                                <div class="row">
                                    <div class="col-md-6">
                                        <div class="info-box bg-light mb-2">
                                            <div class="status-row">
                                                <div class="status-label">Original Size:</div>
                                                <div class="status-value">{original_size_gb:.2f} GB</div>
                                            </div>
                                            <div class="status-row">
                                                <div class="status-label">Compressed Size:</div>
                                                <div class="status-value">{compressed_size_gb:.2f} GB</div>
                                            </div>
                                        </div>
                                    </div>
                                    <div class="col-md-6">
                                        <div class="info-box bg-light">
                                            <div class="status-row">
                                                <div class="status-label">Space Saved:</div>
                                                <div class="status-value">{space_saved_gb:.2f} GB</div>
                                            </div>
                                            <div class="status-row">
                                                <div class="status-label">Savings:</div>
                                                <div class="status-value">{savings_percentage:.1f}%</div>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        
                        <div class="card">
                            <div class="card-header bg-light py-1">
                                <h6 class="mb-0">Time Statistics</h6>
                            </div>
                            <div class="card-body p-3">
                                <div class="row">
                                    <div class="col-md-6">
                                        <div class="info-box bg-light mb-2">
                                            <div class="status-row">
                                                <div class="status-label">Avg. Processing Time:</div>
                                                <div class="status-value">{avg_time_str}</div>
                                            </div>
                                            <div class="status-row">
                                                <div class="status-label">Min Time:</div>
                                                <div class="status-value">{min_time:.1f}s</div>
                                            </div>
                                            <div class="status-row">
                                                <div class="status-label">Max Time:</div>
                                                <div class="status-value">{max_time:.1f}s</div>
                                            </div>
                                        </div>
                                    </div>
                                    <div class="col-md-6">
                                        <div class="info-box bg-light">
                                            <div class="status-row">
                                                <div class="status-label">Estimated Time to Complete All:</div>
                                                <div class="status-value">{eta_str}</div>
                                            </div>
                                            <div class="status-row">
                                                <div class="status-label">Pending Files:</div>
                                                <div class="status-value">{pending_files}</div>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                """
                
            def generate_active_jobs_html(self, active_jobs):
                """Generate HTML for currently active compression jobs."""
                if not active_jobs:
                    return """
                    <div class="text-center py-4">
                        <p class="text-muted">No active compression jobs</p>
                    </div>
                    """
                
                jobs_html = ""
                for job in active_jobs:
                    # Get job details
                    filename = job.get('filename', 'Unknown')
                    full_path = job.get('full_path', 'Unknown')
                    status = job.get('status', 'Unknown')
                    stage = job.get('stage', 'Unknown')
                    progress = job.get('progress', 0)
                    size_mb = job.get('size_mb', 0)
                    elapsed = job.get('elapsed_formatted', '0s')
                    eta = job.get('eta_formatted', 'Unknown')
                    
                    # Determine stage style based on current stage
                    stage_class = "bg-info"
                    if stage == 'content analysis':
                        stage_class = "bg-primary"
                    elif stage == 'encoding':
                        stage_class = "bg-success"
                    elif stage == 'quality check':
                        stage_class = "bg-warning"
                    elif stage == 'finalizing':
                        stage_class = "bg-dark"
                    
                    # Generate progress bar for this job
                    progress_html = ""
                    if progress > 0:
                        progress_html = f"""
                        <div class="progress mt-2" style="height: 12px;">
                            <div class="progress-bar progress-bar-striped progress-bar-animated" 
                                role="progressbar" 
                                style="width: {progress}%;" 
                                aria-valuenow="{progress}" 
                                aria-valuemin="0" 
                                aria-valuemax="100">
                                {progress:.1f}%
                            </div>
                        </div>
                        """
                    
                    jobs_html += f"""
                    <div class="card mb-3">
                        <div class="card-header bg-light py-2">
                            <div class="d-flex justify-content-between align-items-center">
                                <h6 class="mb-0">{filename}</h6>
                                <span class="badge {stage_class}">{stage.upper()}</span>
                            </div>
                        </div>
                        <div class="card-body pt-2 pb-2">
                            <div class="file-path mb-2">{full_path}</div>
                            
                            <div class="row mb-2">
                                <div class="col-md-3">
                                    <small class="text-muted d-block">Size:</small>
                                    <span>{size_mb:.2f} MB</span>
                                </div>
                                <div class="col-md-3">
                                    <small class="text-muted d-block">Status:</small>
                                    <span>{status.upper()}</span>
                                </div>
                                <div class="col-md-3">
                                    <small class="text-muted d-block">Elapsed:</small>
                                    <span>{elapsed}</span>
                                </div>
                                <div class="col-md-3">
                                    <small class="text-muted d-block">ETA:</small>
                                    <span>{eta}</span>
                                </div>
                            </div>
                            
                            {progress_html}
                        </div>
                    </div>
                    """
                
                return jobs_html
                
            def generate_logs_html(self, events):
                """Generate HTML for system logs in a scrollable container."""
                if not events:
                    return """<p class="text-muted p-3">No logs available</p>"""
                
                logs_html = ""
                for event in events:
                    # Get event details
                    event_type = event.get('event_type', 'Unknown')
                    severity = event.get('severity', 'info')
                    timestamp = event.get('timestamp', '')
                    details = event.get('details', '')
                    
                    # Format the timestamp
                    formatted_time = timestamp
                    if timestamp and 'T' in timestamp:
                        date_part, time_part = timestamp.split('T')
                        formatted_time = f"{date_part} {time_part[:8]}"
                    
                    # Set color based on severity
                    severity_color = "text-info"
                    if severity == 'error':
                        severity_color = "text-danger"
                    elif severity == 'warning':
                        severity_color = "text-warning"
                    
                    logs_html += f"""
                    <div class="log-entry">
                        <span class="text-muted">[{formatted_time}]</span>
                        <span class="{severity_color}">[{severity.upper()}]</span>
                        <span class="text-light">{event_type}:</span>
                        <span>{details}</span>
                    </div>
                    """
                
                return logs_html
            
            def generate_scanner_html(self, scanner_status):
                """Generate HTML for scanner status."""
                if scanner_status.get("status") == "idle":
                    return "<p class='text-muted'>Scanner is currently idle</p>"
                
                # Track concurrent scanners in self.active_scanners
                self.active_scanners = []
                
                # If we have multiple scan paths running simultaneously
                for i, path in enumerate(self.scanner.config["media_paths"]):
                    if self.scanner.is_path_being_scanned(path):
                        self.active_scanners.append({
                            "path": path,
                            "progress": self.scanner.get_path_scan_progress(path),
                            "files_scanned": self.scanner.get_files_scanned_in_path(path)
                        })
                
                html = ""
                
                # Add main progress bar
                if scanner_status.get("status") == "scanning":
                    html += f"""
                    <div class="progress mb-3">
                        <div class="progress-bar progress-bar-striped progress-bar-animated" 
                            role="progressbar" 
                            style="width: {scanner_status.get('progress', 0)}%;" 
                            aria-valuenow="{scanner_status.get('progress', 0)}" 
                            aria-valuemin="0" 
                            aria-valuemax="100">
                            {scanner_status.get('progress', 0):.1f}%
                        </div>
                    </div>
                    """
                
                # Add overall scanner details
                html += f"""
                <div class="scanning p-3 rounded mb-3">
                    <div class="row">
                        <div class="col-md-6">
                            <p class="mb-1"><strong>Current Directory:</strong></p>
                            <p class="file-path">{scanner_status.get('current_directory', 'None')}</p>
                        </div>
                        <div class="col-md-6">
                            <div class="row g-2">
                                <div class="col-6">
                                    <div class="border rounded p-2 text-center">
                                        <small class="d-block text-muted">Files Scanned</small>
                                        <span class="fw-bold">{scanner_status.get('files_scanned', 0)}</span>
                                    </div>
                                </div>
                                <div class="col-6">
                                    <div class="border rounded p-2 text-center">
                                        <small class="d-block text-muted">New Files</small>
                                        <span class="fw-bold">{scanner_status.get('new_files', 0)}</span>
                                    </div>
                                </div>
                                <div class="col-6">
                                    <div class="border rounded p-2 text-center">
                                        <small class="d-block text-muted">Changed Files</small>
                                        <span class="fw-bold">{scanner_status.get('changed_files', 0)}</span>
                                    </div>
                                </div>
                                <div class="col-6">
                                    <div class="border rounded p-2 text-center">
                                        <small class="d-block text-muted">Duration</small>
                                        <span class="fw-bold">{scanner_status.get('duration', 0):.1f}s</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                """
                
                # Add ETA if available
                if scanner_status.get('eta_seconds'):
                    # Format ETA
                    eta_seconds = scanner_status['eta_seconds']
                    if eta_seconds < 60:
                        eta_str = f"{int(eta_seconds)}s"
                    elif eta_seconds < 3600:
                        minutes = int(eta_seconds // 60)
                        seconds = int(eta_seconds % 60)
                        eta_str = f"{minutes}m {seconds}s"
                    else:
                        hours = int(eta_seconds // 3600)
                        minutes = int((eta_seconds % 3600) // 60)
                        eta_str = f"{hours}h {minutes}m"
                    
                    html += f"""
                    <div class="mt-2">
                        <p class="mb-1"><strong>Estimated Time Remaining:</strong> {eta_str}</p>
                    </div>
                    """
                
                html += "</div>"
                
                # Add concurrent scanners if any
                if len(self.active_scanners) > 0:
                    html += f"""
                    <h6 class="mt-3">Concurrent Scanners ({len(self.active_scanners)})</h6>
                    """
                    
                    for scanner in self.active_scanners:
                        html += f"""
                        <div class="active-scanner mb-2">
                            <div class="d-flex justify-content-between align-items-center mb-1">
                                <strong>Path:</strong>
                                <span class="badge bg-primary">Scanning</span>
                            </div>
                            <p class="file-path mb-2">{scanner.get('path', 'Unknown')}</p>
                            <div class="progress mb-2" style="height: 8px;">
                                <div class="progress-bar" role="progressbar" 
                                    style="width: {scanner.get('progress', 0)}%;" 
                                    aria-valuenow="{scanner.get('progress', 0)}" 
                                    aria-valuemin="0" 
                                    aria-valuemax="100">
                                </div>
                            </div>
                            <div class="d-flex justify-content-between">
                                <small>{scanner.get('progress', 0):.1f}% complete</small>
                                <small>{scanner.get('files_scanned', 0)} files</small>
                            </div>
                        </div>
                        """
                
                return html
            
            def generate_compressor_html(self, compressor_status):
                """Generate HTML for compressor status."""
                # Generate active jobs HTML
                active_jobs_html = ""
                if compressor_status.get("active_jobs", []):
                    for job in compressor_status["active_jobs"]:
                        # Determine stage style based on current stage
                        stage_class = "bg-info"
                        if job.get('stage') == 'content analysis':
                            stage_class = "bg-primary"
                        elif job.get('stage') == 'encoding':
                            stage_class = "bg-success"
                        elif job.get('stage') == 'quality check':
                            stage_class = "bg-warning"
                        elif job.get('stage') == 'finalizing':
                            stage_class = "bg-dark"
                            
                        active_jobs_html += f"""
                        <div class="compression-job mb-3">
                            <div class="d-flex justify-content-between">
                                <strong>{job.get('filename', 'Unknown')}</strong>
                                <span class="badge {stage_class}">{job.get('stage', 'Unknown')}</span>
                            </div>
                            <div class="file-path">{job.get('full_path', 'Unknown')}</div>
                            
                            <div class="d-flex justify-content-between flex-wrap mt-1 mb-1">
                                <div class="me-2">
                                    <small class="text-muted">Size:</small>
                                    <small class="fw-bold">{job.get('size_mb', 0):.2f} MB</small>
                                </div>
                                <div class="me-2">
                                    <small class="text-muted">Status:</small>
                                    <small class="fw-bold">{job.get('status', 'unknown')}</small>
                                </div>
                                <div>
                                    <small class="text-muted">Runtime:</small>
                                    <small class="fw-bold">{job.get('elapsed_formatted', '0s')}</small>
                                </div>
                            </div>
                        """
                        
                        # Add progress bar if compressing with improved styling
                        if job.get('progress', 0) > 0:
                            # Choose progress bar color based on stage
                            bar_color = "bg-primary"
                            if job.get('stage') == 'encoding':
                                bar_color = "bg-success"
                            elif job.get('stage') == 'quality check':
                                bar_color = "bg-info"
                                
                            active_jobs_html += f"""
                            <div class="progress mt-1" style="height: 10px;">
                                <div class="progress-bar {bar_color}" role="progressbar" 
                                    style="width: {job.get('progress', 0)}%;" 
                                    aria-valuenow="{job.get('progress', 0)}" 
                                    aria-valuemin="0" 
                                    aria-valuemax="100">
                                </div>
                            </div>
                            <div class="d-flex justify-content-between mt-1">
                                <small>{job.get('progress', 0):.1f}% Complete</small>
                            """
                            
                            # Add ETA if available
                            if job.get('eta_formatted', "Unknown") != "Unknown":
                                active_jobs_html += f"""
                                <small>ETA: {job.get('eta_formatted', 'Unknown')}</small>
                                """
                            
                            active_jobs_html += "</div>"
                        
                        active_jobs_html += "</div>"
                else:
                    active_jobs_html = "<p class='text-muted'>No active compression jobs</p>"
                
                # Calculate compression ratio
                original_size = compressor_status.get('total_original_size', 0)
                compressed_size = compressor_status.get('total_compressed_size', 0)
                compression_ratio = 0
                if original_size > 0:
                    compression_ratio = (1 - (compressed_size / original_size)) * 100
                
                # Generate overall status HTML with more detailed stats
                status_html = f"""
                <table class="table table-sm">
                    <tbody>
                        <tr>
                            <th>Status</th>
                            <td>
                                <span class="badge {"bg-warning" if compressor_status.get('paused', False) else "bg-success" if compressor_status.get('status', '') == 'compressing' else "bg-secondary"}">
                                    {compressor_status.get('status', 'UNKNOWN').upper()}
                                </span>
                            </td>
                        </tr>
                        <tr>
                            <th>Running Time</th>
                            <td>{compressor_status.get('duration_formatted', '0s')}</td>
                        </tr>
                        <tr>
                            <th>Files Processed</th>
                            <td>{compressor_status.get('files_processed', 0)}</td>
                        </tr>
                        <tr>
                            <th>Errors</th>
                            <td>{compressor_status.get('errors', 0)}</td>
                        </tr>
                        <tr>
                            <th>Original Size</th>
                            <td>{original_size/1024/1024/1024:.2f} GB</td>
                        </tr>
                        <tr>
                            <th>Compressed Size</th>
                            <td>{compressed_size/1024/1024/1024:.2f} GB</td>
                        </tr>
                        <tr>
                            <th>Space Saved</th>
                            <td>{(original_size - compressed_size)/1024/1024/1024:.2f} GB ({compression_ratio:.1f}%)</td>
                        </tr>
                    </tbody>
                </table>
                """
                
                # Add ETA information with improved styling
                eta_html = ""
                if "eta" in compressor_status and compressor_status.get("status", "") == "compressing":
                    eta_info = compressor_status["eta"]
                    
                    # Create a progress pie or bar visualization of completion
                    percent_done = 0
                    total_files = eta_info.get('total_files', 0) + compressor_status.get('files_processed', 0)
                    if total_files > 0:
                        percent_done = (compressor_status.get('files_processed', 0) / total_files) * 100
                    
                    # Add average time per file if available
                    avg_time = eta_info.get('average_time_per_file', 0)
                    avg_time_str = f"{avg_time:.1f}s" if avg_time < 60 else f"{avg_time/60:.1f}m"
                    
                    eta_html = f"""
                    <div class="card mb-3">
                        <div class="card-header bg-primary text-white">
                            <h6 class="mb-0">Estimated Completion</h6>
                        </div>
                        <div class="card-body">
                            <div class="progress mb-3" style="height: 15px;">
                                <div class="progress-bar bg-success" role="progressbar" 
                                    style="width: {percent_done}%;" 
                                    aria-valuenow="{percent_done}" 
                                    aria-valuemin="0" 
                                    aria-valuemax="100">
                                    {percent_done:.1f}%
                                </div>
                            </div>
                            
                            <div class="row text-center g-2">
                                <div class="col-3">
                                    <div class="border rounded p-2">
                                        <small class="d-block text-muted">Processed</small>
                                        <span class="fw-bold">{compressor_status.get('files_processed', 0)}</span>
                                    </div>
                                </div>
                                <div class="col-3">
                                    <div class="border rounded p-2">
                                        <small class="d-block text-muted">Remaining</small>
                                        <span class="fw-bold">{eta_info.get('total_files', 0)}</span>
                                    </div>
                                </div>
                                <div class="col-3">
                                    <div class="border rounded p-2">
                                        <small class="d-block text-muted">Avg Time/File</small>
                                        <span class="fw-bold">{avg_time_str}</span>
                                    </div>
                                </div>
                                <div class="col-3">
                                    <div class="border rounded p-2 bg-light">
                                        <small class="d-block text-muted">ETA</small>
                                        <span class="fw-bold">{eta_info.get('eta_formatted', 'Unknown')}</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    """
                
                # Add a "Quick Stats" card for current session stats
                quick_stats_html = f"""
                <div class="card mb-3">
                    <div class="card-header">
                        <h6 class="mb-0">Current Session</h6>
                    </div>
                    <div class="card-body p-0">
                        <div class="row g-0 text-center">
                            <div class="col-4 border-end p-2">
                                <small class="d-block text-muted">Processed</small>
                                <span class="fw-bold">{compressor_status.get('files_processed', 0)}</span>
                            </div>
                            <div class="col-4 border-end p-2">
                                <small class="d-block text-muted">Errors</small>
                                <span class="fw-bold text-danger">{compressor_status.get('errors', 0)}</span>
                            </div>
                            <div class="col-4 p-2">
                                <small class="d-block text-muted">Running</small>
                                <span class="fw-bold">{compressor_status.get('duration_formatted', '0s')}</span>
                            </div>
                        </div>
                    </div>
                </div>
                """
                
                # Combine all sections
                html = f"""
                <div class="mb-3">
                    <div class="d-flex justify-content-between align-items-center mb-2">
                        <h6 class="mb-0">Active Jobs ({len(compressor_status.get('active_jobs', []))})</h6>
                        <small class="text-muted">Concurrent tasks: {self.compressor.config.get('max_concurrent_jobs', 1)}</small>
                    </div>
                    {active_jobs_html}
                </div>
                
                {quick_stats_html}
                {eta_html}
                
                <div>
                    <h6>Compression Details</h6>
                    {status_html}
                </div>
                """
                
                return html
                
            def generate_events_html(self, events):
                """Generate HTML for system events."""
                if not events:
                    return "<p class='text-muted'>No recent events</p>"
                    
                events_html = ""
                for event in events:
                    severity = event.get("severity", "info")
                    severity_class = {
                        "error": "event-error",
                        "warning": "event-warning",
                        "info": "event-info"
                    }.get(severity, "")
                    
                    timestamp = event.get("timestamp", "")
                    event_time = timestamp.split("T")[1][:8] if timestamp and "T" in timestamp else timestamp
                    
                    events_html += f"""
                    <div class="event-item {severity_class}">
                        <div class="d-flex justify-content-between">
                            <strong>{event.get("event_type", "Unknown Event")}</strong>
                            <small>{event_time}</small>
                        </div>
                        <div>{event.get("details", "")}</div>
                    </div>
                    """
                
                return events_html
            
            def log_message(self, format, *args):
                """Override to reduce HTTP server logging noise"""
                # Only log errors, not normal requests
                if args[1][0] == '4' or args[1][0] == '5':  # 4xx or 5xx status codes
                    super().log_message(format, *args)
        
        def run_server():
            """Run the web server."""
            server_address = (host, port)
            httpd = HTTPServer(server_address, AuthHandler)
            httpd.web_server = self
            logger.info(f"Starting web interface at http://{host}:{port}/")
            self.server = httpd
            httpd.serve_forever()
        
        # Start the server in a daemon thread
        thread = threading.Thread(target=run_server, daemon=True)
        thread.start()
    
    # 10. Configuration Hot-Reloading
    def reload_configuration(self, config_path: str = None) -> Dict:
        """Reload configuration without restarting."""
        try:
            # Use the existing config path if none provided
            if not config_path:
                # Try to find config path from command line arguments
                import sys
                for i, arg in enumerate(sys.argv):
                    if arg in ['-c', '--config'] and i + 1 < len(sys.argv):
                        config_path = sys.argv[i + 1]
                        break
            
            if not config_path or not os.path.exists(config_path):
                return {"success": False, "message": "Configuration file not found"}
            
            with open(config_path, 'r') as f:
                new_config = json.load(f)
            
            # Validate the new configuration
            is_valid, errors = ConfigValidator.validate_config(new_config)
            
            if not is_valid:
                error_msg = "Invalid configuration: " + "; ".join(errors)
                logger.error(error_msg)
                return {"success": False, "message": error_msg}
            
            # Backup the old configuration
            old_config = self.config.copy()
            
            # Update all components with the new configuration
            self._update_config_recursively(self.config, new_config)
            
            # Update components with new config
            self.db.backup_path = self.config.get("backup_path", self.db.backup_path)
            
            logger.info("Configuration reloaded successfully")
            self.db.log_system_event(
                "config_reloaded",
                "Configuration reloaded successfully",
                "info"
            )
            
            return {"success": True, "message": "Configuration reloaded successfully"}
            
        except Exception as e:
            error_msg = f"Failed to reload configuration: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "message": error_msg}
    
    def _update_config_recursively(self, dest, source):
        """Recursively update nested dictionaries."""
        for key, value in source.items():
            if isinstance(value, dict) and key in dest and isinstance(dest[key], dict):
                self._update_config_recursively(dest[key], value)
            else:
                dest[key] = value
