# DanData TaskMaster

Professional task management and monitoring system for DanData accounting application.

## Features
- 24/7 system health monitoring with real Supabase queries
- Automated database cleanup (soft-deleted records)
- Performance analytics (record counts, user activity, data integrity)
- Cost/usage monitoring (storage, record growth)
- Web dashboard interface
- API key authentication for protected endpoints
- Restricted CORS (only known frontend origins)

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `VITE_SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Yes | Supabase service role key |
| `TASKMASTER_API_KEY` | Recommended | API key for protected endpoints (set in Railway) |
| `ALLOWED_ORIGINS` | Optional | Comma-separated allowed CORS origins |
| `PORT` | Auto | Set by Railway/Heroku automatically |

## Endpoints

Public (no auth required):
- `/` - Service info
- `/health` - Health check
- `/dashboard` - Web interface

Protected (requires `X-API-Key` header):
- `/status` - Task manager status
- `/jobs` - Scheduled jobs
- `/run/{task_name}` - Manual task execution

## Deployment

Deployed on Railway from: https://github.com/starlightkristen/DanData-TaskMaster