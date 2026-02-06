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

from task_supabase_integration import supabase_integration

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
            results = await supabase_integration.cleanup_old_data(retention_days=30)

            if 'error' in results:
                logger.warning(f"Database cleanup had errors: {results['error']}")
                await self._send_alert('db_cleanup_partial', results['error'], results)
            else:
                logger.info(f"Database cleanup completed: {results}")

        except Exception as e:
            logger.error(f"Database cleanup task failed: {str(e)}")
            await self._send_alert('db_cleanup_failed', str(e))

    async def _backup_verification_task(self):
        """Verify database connectivity and data integrity as backup proxy"""
        task_id = f"backup_verify_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting backup verification task: {task_id}")

        try:
            # Verify database is reachable and data looks intact
            health = await supabase_integration.get_system_health()
            integrity = await supabase_integration.check_data_integrity()

            verification_results = {
                "database_status": health.get('services', {}).get('database', 'unknown'),
                "edge_functions_status": health.get('services', {}).get('edge_functions', 'unknown'),
                "integrity_issues": integrity.get('issues_found', []),
                "checks_performed": integrity.get('checks_performed', []),
                "timestamp": datetime.now().isoformat()
            }

            if verification_results['database_status'] != 'healthy':
                await self._send_alert(
                    'backup_verification_warning',
                    f"Database status: {verification_results['database_status']}",
                    verification_results
                )

            logger.info(f"Backup verification completed: {verification_results}")

        except Exception as e:
            logger.error(f"Backup verification task failed: {str(e)}")
            await self._send_alert('backup_verification_failed', str(e))

    async def _performance_analytics_task(self):
        """Gather and analyze real database/storage metrics"""
        task_id = f"perf_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting performance analytics task: {task_id}")

        try:
            db_metrics = await supabase_integration.get_database_metrics()
            user_activity = await supabase_integration.get_user_activity_metrics()
            data_integrity = await supabase_integration.check_data_integrity()

            metrics = {
                "database": db_metrics,
                "user_activity": user_activity,
                "data_integrity": data_integrity,
            }

            # Alert if data integrity issues found
            issues = data_integrity.get('issues_found', [])
            if issues:
                await self._send_alert(
                    'data_integrity_issues',
                    f"Found {len(issues)} data integrity issues",
                    metrics
                )

            logger.info(f"Performance analytics completed: {json.dumps(db_metrics, default=str)}")

        except Exception as e:
            logger.error(f"Performance analytics task failed: {str(e)}")
            await self._send_alert('performance_analytics_failed', str(e))

    async def _cost_monitoring_task(self):
        """Monitor storage usage and record counts as cost proxies"""
        task_id = f"cost_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting cost monitoring task: {task_id}")

        try:
            db_metrics = await supabase_integration.get_database_metrics()
            storage_metrics = await supabase_integration.get_storage_metrics()

            cost_data = {
                "database_record_counts": db_metrics,
                "storage": storage_metrics,
                "timestamp": datetime.now().isoformat()
            }

            # Alert if any table exceeds a row-count threshold (proxy for growth)
            for key, val in db_metrics.items():
                if key.endswith('_count') and isinstance(val, (int, float)) and val > 10000:
                    await self._send_alert(
                        'high_record_count',
                        f"{key} has {val} records - review retention policy",
                        cost_data
                    )

            logger.info(f"Cost monitoring completed: {json.dumps(db_metrics, default=str)}")

        except Exception as e:
            logger.error(f"Cost monitoring task failed: {str(e)}")
            await self._send_alert('cost_monitoring_failed', str(e))

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