#!/usr/bin/env python3
"""
DanData Task Manager Web Interface
Simple web interface for monitoring and managing background tasks
"""

import asyncio
import json
from datetime import datetime
from typing import Dict, Any
from aiohttp import web, web_request
import aiohttp_cors
import logging

from task_manager import DanDataTaskManager

logger = logging.getLogger('dandata.web_interface')

class TaskManagerWebInterface:
    def __init__(self, task_manager: DanDataTaskManager):
        self.task_manager = task_manager
        self.app = web.Application()
        self._setup_routes()
        self._setup_cors()
    
    def _setup_routes(self):
        """Setup web routes"""
        self.app.router.add_get('/', self.index)
        self.app.router.add_get('/status', self.status)
        self.app.router.add_get('/jobs', self.list_jobs)
        self.app.router.add_post('/run/{task_name}', self.run_task)
        self.app.router.add_get('/health', self.health_check)
        
        # Serve static dashboard
        self.app.router.add_get('/dashboard', self.dashboard)
    
    def _setup_cors(self):
        """Setup CORS for frontend integration"""
        cors = aiohttp_cors.setup(self.app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
                allow_methods="*"
            )
        })
        
        for route in list(self.app.router.routes()):
            cors.add(route)
    
    async def index(self, request: web_request.Request) -> web.Response:
        """Root endpoint with basic info"""
        return web.json_response({
            "service": "DanData Task Manager",
            "version": "1.0.0",
            "status": "running",
            "endpoints": {
                "/status": "Get task manager status",
                "/jobs": "List scheduled jobs", 
                "/run/{task_name}": "Run a specific task",
                "/health": "Perform health check",
                "/dashboard": "Web dashboard"
            }
        })
    
    async def status(self, request: web_request.Request) -> web.Response:
        """Get task manager status"""
        try:
            status = self.task_manager.get_task_status()
            return web.json_response(status)
        except Exception as e:
            logger.error(f"Error getting status: {str(e)}")
            return web.json_response({"error": str(e)}, status=500)
    
    async def list_jobs(self, request: web_request.Request) -> web.Response:
        """List all scheduled jobs"""
        try:
            jobs = self.task_manager.list_scheduled_jobs()
            return web.json_response(jobs)
        except Exception as e:
            logger.error(f"Error listing jobs: {str(e)}")
            return web.json_response({"error": str(e)}, status=500)
    
    async def run_task(self, request: web_request.Request) -> web.Response:
        """Manually run a specific task"""
        task_name = request.match_info['task_name']
        
        try:
            # Map task names to methods
            task_methods = {
                'health_check': self.task_manager._health_check_task,
                'database_cleanup': self.task_manager._database_cleanup_task,
                'backup_verification': self.task_manager._backup_verification_task,
                'performance_analytics': self.task_manager._performance_analytics_task,
                'cost_monitoring': self.task_manager._cost_monitoring_task
            }
            
            if task_name not in task_methods:
                return web.json_response(
                    {"error": f"Unknown task: {task_name}"}, 
                    status=400
                )
            
            # Run the task
            await task_methods[task_name]()
            
            return web.json_response({
                "message": f"Task {task_name} completed successfully",
                "timestamp": datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Error running task {task_name}: {str(e)}")
            return web.json_response({"error": str(e)}, status=500)
    
    async def health_check(self, request: web_request.Request) -> web.Response:
        """Perform immediate health check"""
        return await self.run_task(web_request.Request({
            'match_info': {'task_name': 'health_check'}
        }))
    
    async def dashboard(self, request: web_request.Request) -> web.Response:
        """Serve simple HTML dashboard"""
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>DanData Task Manager</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body { font-family: -apple-system, sans-serif; margin: 40px; line-height: 1.6; }
                .container { max-width: 800px; margin: 0 auto; }
                .status { padding: 20px; background: #f5f5f5; border-radius: 8px; margin-bottom: 20px; }
                .jobs { background: #fff; border: 1px solid #ddd; border-radius: 8px; }
                .job { padding: 15px; border-bottom: 1px solid #eee; }
                .job:last-child { border-bottom: none; }
                button { background: #007bff; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; }
                button:hover { background: #0056b3; }
                .error { color: #dc3545; background: #f8d7da; padding: 10px; border-radius: 4px; }
                .success { color: #155724; background: #d4edda; padding: 10px; border-radius: 4px; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🚀 DanData Task Manager</h1>
                
                <div class="status">
                    <h2>System Status</h2>
                    <div id="status-info">Loading...</div>
                    <button onclick="refreshStatus()">Refresh Status</button>
                </div>
                
                <div class="jobs">
                    <h2>Scheduled Jobs</h2>
                    <div id="jobs-list">Loading...</div>
                </div>
                
                <div style="margin-top: 20px;">
                    <h2>Quick Actions</h2>
                    <button onclick="runTask('health_check')">Run Health Check</button>
                    <button onclick="runTask('database_cleanup')">Database Cleanup</button>
                    <button onclick="runTask('backup_verification')">Verify Backups</button>
                </div>
                
                <div id="messages" style="margin-top: 20px;"></div>
            </div>
            
            <script>
                async function fetchData(url) {
                    try {
                        const response = await fetch(url);
                        return await response.json();
                    } catch (error) {
                        console.error('Fetch error:', error);
                        return { error: error.message };
                    }
                }
                
                async function refreshStatus() {
                    const status = await fetchData('/status');
                    const statusDiv = document.getElementById('status-info');
                    
                    if (status.error) {
                        statusDiv.innerHTML = `<div class="error">Error: ${status.error}</div>`;
                    } else {
                        statusDiv.innerHTML = `
                            <p><strong>Scheduler:</strong> ${status.scheduler_running ? 'Running' : 'Stopped'}</p>
                            <p><strong>Jobs:</strong> ${status.jobs_count}</p>
                            <p><strong>Health Status:</strong> ${status.health_status}</p>
                            <p><strong>Last Health Check:</strong> ${status.last_health_check || 'Never'}</p>
                        `;
                    }
                }
                
                async function refreshJobs() {
                    const jobs = await fetchData('/jobs');
                    const jobsDiv = document.getElementById('jobs-list');
                    
                    if (jobs.error) {
                        jobsDiv.innerHTML = `<div class="error">Error: ${jobs.error}</div>`;
                    } else {
                        jobsDiv.innerHTML = jobs.map(job => `
                            <div class="job">
                                <strong>${job.name}</strong> (${job.id})
                                <br>Next run: ${job.next_run || 'Not scheduled'}
                                <br>Trigger: ${job.trigger}
                            </div>
                        `).join('');
                    }
                }
                
                async function runTask(taskName) {
                    showMessage('Running task: ' + taskName + '...', 'info');
                    
                    try {
                        const response = await fetch(`/run/${taskName}`, { method: 'POST' });
                        const result = await response.json();
                        
                        if (response.ok) {
                            showMessage(`Task ${taskName} completed successfully`, 'success');
                            refreshStatus();
                        } else {
                            showMessage(`Task ${taskName} failed: ${result.error}`, 'error');
                        }
                    } catch (error) {
                        showMessage(`Task ${taskName} failed: ${error.message}`, 'error');
                    }
                }
                
                function showMessage(message, type) {
                    const messagesDiv = document.getElementById('messages');
                    const messageDiv = document.createElement('div');
                    messageDiv.className = type;
                    messageDiv.textContent = message;
                    messagesDiv.appendChild(messageDiv);
                    
                    setTimeout(() => {
                        messagesDiv.removeChild(messageDiv);
                    }, 5000);
                }
                
                // Initial load
                refreshStatus();
                refreshJobs();
                
                // Auto-refresh every 30 seconds
                setInterval(refreshStatus, 30000);
                setInterval(refreshJobs, 60000);
            </script>
        </body>
        </html>
        """
        return web.Response(text=html, content_type='text/html')

async def create_web_server(task_manager: DanDataTaskManager, port: int = 8000):
    """Create and start the web server"""
    interface = TaskManagerWebInterface(task_manager)
    runner = web.AppRunner(interface.app)
    await runner.setup()
    
    site = web.TCPSite(runner, 'localhost', port)
    await site.start()
    
    logger.info(f"Task Manager web interface started on http://localhost:{port}")
    logger.info(f"Dashboard available at http://localhost:{port}/dashboard")
    
    return runner

async def main():
    """Main entry point with web interface"""
    # Start task manager
    task_manager = DanDataTaskManager()
    
    # Start web interface
    web_runner = await create_web_server(task_manager, port=8000)
    
    try:
        # Start task manager (this will run forever)
        await task_manager.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await web_runner.cleanup()
        task_manager.scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(main())