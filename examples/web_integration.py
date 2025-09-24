"""
Web framework integration examples for Enterprise Job Orchestrator

This example shows how to integrate the job orchestrator with popular web frameworks.
"""

import asyncio
import uuid
from datetime import datetime
from typing import Dict, Any

from enterprise_job_orchestrator import (
    JobOrchestrator,
    Job,
    JobType,
    JobPriority,
    DatabaseManager
)


# Flask Integration Example
def create_flask_app():
    """Create Flask app with job orchestrator integration."""
    try:
        from flask import Flask, request, jsonify
    except ImportError:
        print("Flask not installed. Install with: pip install flask")
        return None

    app = Flask(__name__)
    orchestrator = None

    @app.before_first_request
    def setup_orchestrator():
        """Initialize orchestrator when Flask starts."""
        global orchestrator
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def init():
            nonlocal orchestrator
            db_manager = DatabaseManager("postgresql://localhost/job_orchestrator")
            await db_manager.initialize()
            orchestrator = JobOrchestrator(db_manager)
            await orchestrator.start()

        loop.run_until_complete(init())

    @app.route('/jobs', methods=['POST'])
    def submit_job():
        """Submit a new job via REST API."""
        data = request.get_json()

        job = Job(
            job_id=f"api_job_{uuid.uuid4().hex[:8]}",
            job_name=data.get('name', 'API Job'),
            job_type=JobType(data.get('type', 'data_processing')),
            priority=JobPriority(data.get('priority', 'normal')),
            config=data.get('config', {})
        )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        job_id = loop.run_until_complete(orchestrator.submit_job(job))

        return jsonify({
            'job_id': job_id,
            'status': 'submitted',
            'message': 'Job submitted successfully'
        })

    @app.route('/jobs/<job_id>', methods=['GET'])
    def get_job_status(job_id):
        """Get job status via REST API."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        status = loop.run_until_complete(orchestrator.get_job_status(job_id))

        if status:
            return jsonify(status)
        else:
            return jsonify({'error': 'Job not found'}), 404

    @app.route('/health', methods=['GET'])
    def health_check():
        """System health endpoint."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        health = loop.run_until_complete(orchestrator.get_system_health())

        return jsonify(health)

    return app


# FastAPI Integration Example
def create_fastapi_app():
    """Create FastAPI app with job orchestrator integration."""
    try:
        from fastapi import FastAPI, HTTPException
        from pydantic import BaseModel
    except ImportError:
        print("FastAPI not installed. Install with: pip install fastapi uvicorn")
        return None

    app = FastAPI(title="Job Orchestrator API", version="1.0.0")
    orchestrator = None

    class JobRequest(BaseModel):
        name: str
        type: str = "data_processing"
        priority: str = "normal"
        config: Dict[str, Any] = {}

    class JobResponse(BaseModel):
        job_id: str
        status: str
        message: str

    @app.on_event("startup")
    async def startup_event():
        """Initialize orchestrator when FastAPI starts."""
        global orchestrator
        db_manager = DatabaseManager("postgresql://localhost/job_orchestrator")
        await db_manager.initialize()
        orchestrator = JobOrchestrator(db_manager)
        await orchestrator.start()

    @app.on_event("shutdown")
    async def shutdown_event():
        """Cleanup when FastAPI shuts down."""
        if orchestrator:
            await orchestrator.stop()

    @app.post("/jobs", response_model=JobResponse)
    async def submit_job(job_request: JobRequest):
        """Submit a new job via REST API."""
        job = Job(
            job_id=f"api_job_{uuid.uuid4().hex[:8]}",
            job_name=job_request.name,
            job_type=JobType(job_request.type.upper()),
            priority=JobPriority(job_request.priority.upper()),
            config=job_request.config
        )

        job_id = await orchestrator.submit_job(job)

        return JobResponse(
            job_id=job_id,
            status="submitted",
            message="Job submitted successfully"
        )

    @app.get("/jobs/{job_id}")
    async def get_job_status(job_id: str):
        """Get job status via REST API."""
        status = await orchestrator.get_job_status(job_id)

        if status:
            return status
        else:
            raise HTTPException(status_code=404, detail="Job not found")

    @app.get("/health")
    async def health_check():
        """System health endpoint."""
        return await orchestrator.get_system_health()

    @app.get("/workers")
    async def list_workers():
        """List all workers."""
        return await orchestrator.get_workers()

    @app.get("/queue/stats")
    async def queue_statistics():
        """Get queue statistics."""
        return await orchestrator.get_queue_statistics()

    return app


# Django Integration Example
def setup_django_integration():
    """Django integration setup."""
    django_code = '''
# Add to your Django settings.py
INSTALLED_APPS = [
    # ... your other apps
    'job_orchestrator_app',
]

# Add to your Django apps.py
from django.apps import AppConfig
from enterprise_job_orchestrator import JobOrchestrator, DatabaseManager
import asyncio

class JobOrchestratorConfig(AppConfig):
    name = 'job_orchestrator_app'
    orchestrator = None

    def ready(self):
        # Initialize orchestrator when Django starts
        if not self.orchestrator:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def init():
                db_manager = DatabaseManager("postgresql://localhost/job_orchestrator")
                await db_manager.initialize()
                self.orchestrator = JobOrchestrator(db_manager)
                await self.orchestrator.start()

            loop.run_until_complete(init())

# Add to your Django views.py
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.apps import apps
import json
import asyncio

@csrf_exempt
@require_http_methods(["POST"])
def submit_job(request):
    """Submit job via Django view."""
    config = apps.get_app_config('job_orchestrator_app')

    data = json.loads(request.body)

    job = Job(
        job_id=f"django_job_{uuid.uuid4().hex[:8]}",
        job_name=data.get('name', 'Django Job'),
        job_type=JobType(data.get('type', 'data_processing')),
        config=data.get('config', {})
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    job_id = loop.run_until_complete(config.orchestrator.submit_job(job))

    return JsonResponse({
        'job_id': job_id,
        'status': 'submitted'
    })

# Add to your Django urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('api/jobs/', views.submit_job, name='submit_job'),
    # ... other URLs
]
'''

    print("Django Integration Setup:")
    print(django_code)


async def demonstrate_integrations():
    """Demonstrate all integration examples."""
    print("🌐 Web Framework Integration Examples\n")

    # Flask Example
    print("1. Flask Integration:")
    flask_app = create_flask_app()
    if flask_app:
        print("   ✅ Flask app created successfully")
        print("   📝 To run: flask run")
        print("   🔗 Endpoints: POST /jobs, GET /jobs/<id>, GET /health")
    else:
        print("   ❌ Flask not available")

    print()

    # FastAPI Example
    print("2. FastAPI Integration:")
    fastapi_app = create_fastapi_app()
    if fastapi_app:
        print("   ✅ FastAPI app created successfully")
        print("   📝 To run: uvicorn main:app --reload")
        print("   🔗 Interactive docs: http://localhost:8000/docs")
    else:
        print("   ❌ FastAPI not available")

    print()

    # Django Example
    print("3. Django Integration:")
    setup_django_integration()

    print("\n🎯 Integration examples completed!")


if __name__ == "__main__":
    asyncio.run(demonstrate_integrations())