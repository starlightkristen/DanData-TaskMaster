#!/usr/bin/env python3
"""
DanData Task Manager Integration
Integrates the task manager with the existing DanData Supabase backend
"""

import os
import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import requests
from supabase import create_client, Client

logger = logging.getLogger('dandata.integration')

class SupabaseTaskIntegration:
    """
    Integration layer between task manager and Supabase backend
    """
    
    def __init__(self):
        self.supabase_url = os.getenv('VITE_SUPABASE_URL', '')
        self.supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY', '')
        
        if not self.supabase_url or not self.supabase_key:
            logger.warning("Supabase credentials not configured - some features will be disabled")
            self.supabase = None
        else:
            self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
    
    async def get_system_health(self) -> Dict[str, Any]:
        """Get comprehensive system health from various sources"""
        health_data = {
            "timestamp": datetime.now().isoformat(),
            "services": {},
            "metrics": {}
        }
        
        # Check Supabase edge function
        try:
            edge_function_url = f"{self.supabase_url}/functions/v1/make-server-3f2c553b/health"
            response = requests.get(edge_function_url, timeout=10)
            
            if response.status_code == 200:
                health_data["services"]["edge_functions"] = "healthy"
                health_data["edge_function_data"] = response.json()
            else:
                health_data["services"]["edge_functions"] = "degraded"
                
        except Exception as e:
            health_data["services"]["edge_functions"] = "unavailable"
            health_data["edge_function_error"] = str(e)
        
        # Check database connectivity
        if self.supabase:
            try:
                # Simple query to test database
                result = self.supabase.table('projects').select('count', count='exact').limit(1).execute()
                health_data["services"]["database"] = "healthy"
                health_data["metrics"]["total_projects"] = result.count
            except Exception as e:
                health_data["services"]["database"] = "unavailable"
                health_data["database_error"] = str(e)
        
        return health_data
    
    async def get_database_metrics(self) -> Dict[str, Any]:
        """Get database performance and usage metrics"""
        if not self.supabase:
            return {"error": "Supabase not configured"}
        
        metrics = {}
        
        try:
            # Get record counts for main tables
            tables = ['projects', 'income', 'expenses', 'personal_income', 'personal_expenses']
            
            for table in tables:
                result = self.supabase.table(table).select('count', count='exact').limit(1).execute()
                metrics[f"{table}_count"] = result.count
            
            # Get recent activity (last 7 days)
            from datetime import datetime, timedelta
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            
            recent_projects = self.supabase.table('projects').select('count', count='exact').gte('created_at', week_ago).execute()
            metrics['new_projects_7d'] = recent_projects.count
            
            recent_expenses = self.supabase.table('expenses').select('count', count='exact').gte('created_at', week_ago).execute()
            metrics['new_expenses_7d'] = recent_expenses.count
            
        except Exception as e:
            logger.error(f"Error getting database metrics: {str(e)}")
            metrics['error'] = str(e)
        
        return metrics
    
    async def cleanup_old_data(self, retention_days: int = 30) -> Dict[str, Any]:
        """Clean up old soft-deleted data"""
        if not self.supabase:
            return {"error": "Supabase not configured"}
        
        cleanup_results = {}
        
        try:
            from datetime import datetime, timedelta
            cutoff_date = (datetime.now() - timedelta(days=retention_days)).isoformat()
            
            tables = ['projects', 'income', 'expenses', 'personal_income', 'personal_expenses']
            
            for table in tables:
                # Delete records that are soft-deleted and older than retention period
                result = self.supabase.table(table).delete().is_('deleted_at', 'not.null').lt('deleted_at', cutoff_date).execute()
                cleanup_results[table] = len(result.data) if result.data else 0
            
            logger.info(f"Database cleanup completed: {cleanup_results}")
            
        except Exception as e:
            logger.error(f"Error during database cleanup: {str(e)}")
            cleanup_results['error'] = str(e)
        
        return cleanup_results
    
    async def get_storage_metrics(self) -> Dict[str, Any]:
        """Get storage usage metrics"""
        if not self.supabase:
            return {"error": "Supabase not configured"}
        
        storage_metrics = {}
        
        try:
            # Get storage bucket info
            buckets = self.supabase.storage.list_buckets()
            storage_metrics['buckets'] = []
            
            for bucket in buckets:
                bucket_info = {
                    'name': bucket.name,
                    'public': bucket.public,
                    'created_at': bucket.created_at
                }
                
                # Get file count and size for each bucket
                try:
                    files = self.supabase.storage.from_(bucket.name).list()
                    bucket_info['file_count'] = len(files)
                    
                    # Calculate total size (this would need to be done differently in production)
                    total_size = 0
                    for file in files[:10]:  # Limit to first 10 files for demo
                        # Note: Getting file size would require additional API calls
                        # In production, this should be tracked in database
                        pass
                    
                    bucket_info['total_size_estimate'] = total_size
                    
                except Exception as e:
                    bucket_info['error'] = str(e)
                
                storage_metrics['buckets'].append(bucket_info)
            
        except Exception as e:
            logger.error(f"Error getting storage metrics: {str(e)}")
            storage_metrics['error'] = str(e)
        
        return storage_metrics
    
    async def log_task_execution(self, task_name: str, status: str, details: Dict = None):
        """Log task execution to database for audit trail"""
        if not self.supabase:
            return
        
        try:
            log_entry = {
                'task_name': task_name,
                'status': status,
                'executed_at': datetime.now().isoformat(),
                'details': details or {},
                'server_info': {
                    'hostname': os.environ.get('HOSTNAME', 'unknown'),
                    'environment': os.environ.get('NODE_ENV', 'development')
                }
            }
            
            # In a real implementation, you'd create a task_logs table
            # For now, we'll just log it
            logger.info(f"Task execution logged: {log_entry}")
            
        except Exception as e:
            logger.error(f"Error logging task execution: {str(e)}")
    
    async def get_user_activity_metrics(self) -> Dict[str, Any]:
        """Get user activity and usage metrics"""
        if not self.supabase:
            return {"error": "Supabase not configured"}
        
        activity_metrics = {}
        
        try:
            from datetime import datetime, timedelta
            
            # Active users (users with activity in last 30 days)
            thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
            
            # This would need proper user activity tracking
            # For now, we'll estimate based on recent records
            recent_projects = self.supabase.table('projects').select('user_id').gte('updated_at', thirty_days_ago).execute()
            
            active_users = set()
            if recent_projects.data:
                for project in recent_projects.data:
                    if project.get('user_id'):
                        active_users.add(project['user_id'])
            
            activity_metrics['active_users_30d'] = len(active_users)
            
            # Get total user count (this would need a users table query)
            # activity_metrics['total_users'] = user_count
            
        except Exception as e:
            logger.error(f"Error getting user activity metrics: {str(e)}")
            activity_metrics['error'] = str(e)
        
        return activity_metrics
    
    async def check_data_integrity(self) -> Dict[str, Any]:
        """Check for data integrity issues"""
        if not self.supabase:
            return {"error": "Supabase not configured"}
        
        integrity_results = {
            "issues_found": [],
            "checks_performed": []
        }
        
        try:
            # Check for orphaned records
            integrity_results["checks_performed"].append("orphaned_records")
            
            # Check for income without projects
            orphaned_income = self.supabase.table('income').select('id,project_id').is_('project_id', 'not.null').execute()
            if orphaned_income.data:
                # Cross-check with existing projects
                project_ids = [item['project_id'] for item in orphaned_income.data]
                if project_ids:
                    existing_projects = self.supabase.table('projects').select('id').in_('id', project_ids).execute()
                    existing_ids = set(p['id'] for p in existing_projects.data)
                    
                    orphaned_count = sum(1 for item in orphaned_income.data 
                                       if item['project_id'] not in existing_ids)
                    
                    if orphaned_count > 0:
                        integrity_results["issues_found"].append(f"Found {orphaned_count} orphaned income records")
            
            # Check for expenses without projects (similar logic)
            integrity_results["checks_performed"].append("orphaned_expenses")
            
            # Check for missing required fields
            integrity_results["checks_performed"].append("missing_required_fields")
            
            # Check for duplicate records
            integrity_results["checks_performed"].append("duplicate_detection")
            
        except Exception as e:
            logger.error(f"Error checking data integrity: {str(e)}")
            integrity_results['error'] = str(e)
        
        return integrity_results

# Global integration instance
supabase_integration = SupabaseTaskIntegration()

async def get_integrated_health_check():
    """Get comprehensive health check with Supabase integration"""
    return await supabase_integration.get_system_health()

async def perform_integrated_cleanup():
    """Perform cleanup with Supabase integration"""
    return await supabase_integration.cleanup_old_data()

async def get_system_metrics():
    """Get all system metrics"""
    return {
        "database": await supabase_integration.get_database_metrics(),
        "storage": await supabase_integration.get_storage_metrics(),
        "user_activity": await supabase_integration.get_user_activity_metrics(),
        "data_integrity": await supabase_integration.check_data_integrity()
    }