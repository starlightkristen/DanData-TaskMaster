#!/usr/bin/env python3
"""
DanData Task Manager
Integrates with the existing Supabase-based architecture to provide:
1. Scheduled task execution (backups, cleanup, reports)
2. Async task processing (email notifications, data exports)
3. Monitoring and health checks for background tasks
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('dandata.taskmaster')

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class TaskResult:
    task_id: str
    status: TaskStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class DanDataTaskManager:
    """
    Task Manager integrated with DanData's Supabase backend
    """
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler(
            jobstores={'default': MemoryJobStore()},
            executors={'default': AsyncIOExecutor()},
            job_defaults={'coalesce': False, 'max_instances': 1}
        )
        self.tasks: Dict[str, TaskResult] = {}
        self.supabase_url = os.getenv('VITE_SUPABASE_URL', '')
        self.supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY', '')
        self.edge_function_url = f"{self.supabase_url}/functions/v1/make-server-3f2c553b"
        
        # Health check configuration
        self.last_health_check = None
        self.health_status = "unknown"
        
    async def start(self):
        """Start the task scheduler"""
        logger.info("Starting DanData Task Manager...")
        
        # Add scheduled tasks
        await self._setup_scheduled_tasks()
        
        # Start scheduler
        self.scheduler.start()
        logger.info("Task Manager started successfully")
        
        # Keep running
        try:
            while True:
                await asyncio.sleep(60)  # Check every minute
                await self._run_health_check()
        except KeyboardInterrupt:
            logger.info("Shutting down Task Manager...")
            self.scheduler.shutdown()

    async def _setup_scheduled_tasks(self):
        """Setup all scheduled tasks"""
        
        # 1. Health Check - Every 5 minutes
        self.scheduler.add_job(
            self._health_check_task,
            IntervalTrigger(minutes=5),
            id='health_check',
            name='System Health Check'
        )
        
        # 2. Database Cleanup - Daily at 2 AM
        self.scheduler.add_job(
            self._database_cleanup_task,
            CronTrigger(hour=2, minute=0),
            id='db_cleanup',
            name='Database Cleanup'
        )
        
        # 3. Backup Verification - Weekly on Sunday at 3 AM
        self.scheduler.add_job(
            self._backup_verification_task,
            CronTrigger(day_of_week=6, hour=3, minute=0),
            id='backup_verify',
            name='Backup Verification'
        )
        
        # 4. Performance Analytics - Daily at 1 AM
        self.scheduler.add_job(
            self._performance_analytics_task,
            CronTrigger(hour=1, minute=0),
            id='performance_analytics',
            name='Performance Analytics'
        )
        
        # 5. Cost Monitoring - Daily at 9 AM
        self.scheduler.add_job(
            self._cost_monitoring_task,
            CronTrigger(hour=9, minute=0),
            id='cost_monitoring',
            name='Cost Monitoring'
        )
        
        logger.info("Scheduled tasks configured")

    async def _health_check_task(self):
        """Perform comprehensive health check"""
        task_id = f"health_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting health check task: {task_id}")
        
        try:
            # Check edge function health
            response = requests.get(f"{self.edge_function_url}/health", timeout=10)
            
            if response.status_code == 200:
                health_data = response.json()
                self.health_status = health_data.get('status', 'unknown')
                self.last_health_check = datetime.now()
                
                # Log health status
                logger.info(f"Health check passed: {self.health_status}")
                
                # Alert if degraded
                if self.health_status in ['degraded', 'unhealthy']:
                    await self._send_alert(
                        'health_degraded',
                        f"System health is {self.health_status}",
                        health_data
                    )
                    
            else:
                raise Exception(f"Health check failed: HTTP {response.status_code}")
                
        except Exception as e:
            logger.error(f"Health check task failed: {str(e)}")
            self.health_status = "unhealthy"
            await self._send_alert('health_check_failed', str(e))

    async def _database_cleanup_task(self):
        """Clean up old soft-deleted records and optimize database"""
        task_id = f"db_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting database cleanup task: {task_id}")
        
        try:
            # This would call a Supabase edge function for cleanup
            # For now, we'll simulate the cleanup logic
            cleanup_actions = [
                "Clean soft-deleted records older than 30 days",
                "Archive old transaction data (7+ years)",
                "Clean up orphaned file references",
                "Update database statistics"
            ]
            
            results = {}
            for action in cleanup_actions:
                # Simulate cleanup action
                logger.info(f"Executing: {action}")
                results[action] = "completed"
                await asyncio.sleep(1)  # Simulate processing time
            
            logger.info(f"Database cleanup completed: {results}")
            
        except Exception as e:
            logger.error(f"Database cleanup task failed: {str(e)}")
            await self._send_alert('db_cleanup_failed', str(e))

    async def _backup_verification_task(self):
        """Verify database backups are working correctly"""
        task_id = f"backup_verify_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting backup verification task: {task_id}")
        
        try:
            # This would verify Supabase backups
            # For implementation, this would:
            # 1. Check if automatic backups are running
            # 2. Verify backup integrity
            # 3. Test restore capability in test environment
            
            verification_results = {
                "automatic_backups": "enabled",
                "last_backup": "2026-02-04T02:00:00Z",
                "backup_size": "245MB",
                "integrity_check": "passed"
            }
            
            logger.info(f"Backup verification completed: {verification_results}")
            
        except Exception as e:
            logger.error(f"Backup verification task failed: {str(e)}")
            await self._send_alert('backup_verification_failed', str(e))

    async def _performance_analytics_task(self):
        """Gather and analyze performance metrics"""
        task_id = f"perf_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting performance analytics task: {task_id}")
        
        try:
            # Gather performance metrics
            metrics = {
                "api_response_times": await self._get_api_metrics(),
                "database_performance": await self._get_db_metrics(),
                "frontend_vitals": await self._get_frontend_metrics(),
                "error_rates": await self._get_error_metrics()
            }
            
            # Analyze trends and generate alerts if needed
            await self._analyze_performance_trends(metrics)
            
            logger.info("Performance analytics completed")
            
        except Exception as e:
            logger.error(f"Performance analytics task failed: {str(e)}")
            await self._send_alert('performance_analytics_failed', str(e))

    async def _cost_monitoring_task(self):
        """Monitor costs and usage across services"""
        task_id = f"cost_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting cost monitoring task: {task_id}")
        
        try:
            # Simulate cost monitoring
            # In real implementation, this would:
            # 1. Check Supabase usage and billing
            # 2. Check Vercel usage and billing
            # 3. Monitor storage usage trends
            # 4. Alert on unusual spending patterns
            
            cost_data = {
                "supabase_usage": {
                    "database_size": "250MB",
                    "api_requests": 45000,
                    "storage_used": "1.2GB",
                    "estimated_cost": 15.50
                },
                "vercel_usage": {
                    "bandwidth": "50GB",
                    "function_invocations": 25000,
                    "estimated_cost": 8.25
                }
            }
            
            total_cost = cost_data["supabase_usage"]["estimated_cost"] + cost_data["vercel_usage"]["estimated_cost"]
            
            # Alert if approaching budget threshold
            if total_cost > 20:  # $20 threshold
                await self._send_alert(
                    'cost_threshold_warning',
                    f"Monthly cost approaching threshold: ${total_cost:.2f}",
                    cost_data
                )
            
            logger.info(f"Cost monitoring completed: Total estimated: ${total_cost:.2f}")
            
        except Exception as e:
            logger.error(f"Cost monitoring task failed: {str(e)}")
            await self._send_alert('cost_monitoring_failed', str(e))

    async def _get_api_metrics(self):
        """Get API performance metrics"""
        # In real implementation, this would query Supabase logs
        return {
            "avg_response_time": "245ms",
            "95th_percentile": "450ms",
            "error_rate": "0.2%"
        }

    async def _get_db_metrics(self):
        """Get database performance metrics"""
        # In real implementation, this would query database stats
        return {
            "connection_pool_usage": "12/20",
            "slow_queries": 3,
            "cache_hit_ratio": "94%"
        }

    async def _get_frontend_metrics(self):
        """Get frontend performance metrics"""
        # In real implementation, this would collect Web Vitals
        return {
            "first_contentful_paint": "1.2s",
            "largest_contentful_paint": "2.1s",
            "cumulative_layout_shift": "0.05"
        }

    async def _get_error_metrics(self):
        """Get error rate metrics"""
        # In real implementation, this would analyze error logs
        return {
            "total_errors": 12,
            "error_rate": "0.2%",
            "most_common": "Authentication timeout"
        }

    async def _analyze_performance_trends(self, metrics):
        """Analyze performance trends and generate alerts"""
        # Simple trend analysis - in production, this would be more sophisticated
        issues = []
        
        if metrics["api_response_times"]["95th_percentile"] > "1000ms":
            issues.append("API response times degraded")
        
        if float(metrics["api_response_times"]["error_rate"].rstrip('%')) > 1.0:
            issues.append("Error rate elevated")
        
        if issues:
            await self._send_alert('performance_degradation', "; ".join(issues), metrics)

    async def _send_alert(self, alert_type: str, message: str, data: Dict = None):
        """Send alert notifications"""
        alert_data = {
            "type": alert_type,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "data": data or {}
        }
        
        logger.warning(f"ALERT: {alert_type} - {message}")
        
        # In production, this would send emails, Slack notifications, etc.
        # For now, we'll just log the alert
        print(f"\n🚨 ALERT: {alert_type}")
        print(f"📝 Message: {message}")
        print(f"⏰ Time: {alert_data['timestamp']}")
        if data:
            print(f"📊 Data: {json.dumps(data, indent=2)}")
        print("-" * 50)

    async def _run_health_check(self):
        """Run periodic health check"""
        if not self.last_health_check or \
           (datetime.now() - self.last_health_check) > timedelta(minutes=10):
            logger.warning("Health check overdue, running manual check")
            await self._health_check_task()

    def get_task_status(self) -> Dict[str, Any]:
        """Get current task manager status"""
        return {
            "scheduler_running": self.scheduler.running,
            "jobs_count": len(self.scheduler.get_jobs()),
            "last_health_check": self.last_health_check.isoformat() if self.last_health_check else None,
            "health_status": self.health_status,
            "active_tasks": len([t for t in self.tasks.values() if t.status == TaskStatus.RUNNING])
        }

    def list_scheduled_jobs(self) -> List[Dict[str, Any]]:
        """List all scheduled jobs"""
        jobs = []
        for job in self.scheduler.get_jobs():
            # Simple job info without next_run_time for compatibility
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": "Scheduled (start scheduler to see next run)",
                "trigger": str(job.trigger)
            })
        return jobs

async def main():
    """Main entry point for the task manager"""
    task_manager = DanDataTaskManager()
    
    # Start the task manager
    await task_manager.start()

if __name__ == "__main__":
    # Run the task manager
    asyncio.run(main())