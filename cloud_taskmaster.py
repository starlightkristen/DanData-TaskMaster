#!/usr/bin/env python3
"""
Enhanced TaskMaster for Cloud Deployment
Optimized for Railway.app, Render, Heroku, etc.
"""

import os
import asyncio
import logging
from datetime import datetime

# Load environment variables from .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not required in production

from task_web_interface import TaskManagerWebInterface
from task_manager import DanDataTaskManager

# Configure logging for cloud deployment
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger('dandata.cloud')

async def main():
    """Cloud-optimized main entry point"""
    
    # Get port from environment (Railway/Heroku set this)
    port = int(os.getenv('PORT', 8000))
    
    logger.info(f"Starting DanData TaskMaster on port {port}")
    logger.info(f"Environment: {os.getenv('NODE_ENV', 'development')}")
    
    # Verify environment variables
    supabase_url = os.getenv('VITE_SUPABASE_URL')
    service_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    
    if not supabase_url or not service_key:
        logger.error("Missing required environment variables:")
        logger.error("  VITE_SUPABASE_URL: " + ("SET" if supabase_url else "MISSING"))
        logger.error("  SUPABASE_SERVICE_ROLE_KEY: " + ("SET" if service_key else "MISSING"))
        logger.error("Please set these environment variables in your deployment platform")
        return
    
    # Create task manager
    task_manager = DanDataTaskManager()
    
    # Create web interface  
    web_interface = TaskManagerWebInterface(task_manager)
    
    # Start web server
    from aiohttp import web
    runner = web.AppRunner(web_interface.app)
    await runner.setup()
    
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    logger.info(f"✅ TaskMaster web interface started on http://0.0.0.0:{port}")
    logger.info(f"📊 Dashboard available at http://0.0.0.0:{port}/dashboard")
    
    try:
        # Start task manager (this will run forever)
        await task_manager.start()
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down TaskMaster...")
        await runner.cleanup()
        task_manager.scheduler.shutdown()
    except Exception as e:
        logger.error(f"❌ TaskMaster crashed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())