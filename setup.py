"""
Setup script for Enterprise Job Orchestrator

A comprehensive, enterprise-grade job orchestration system designed for processing
massive datasets with distributed workers, fault tolerance, and comprehensive monitoring.
"""

from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
try:
    long_description = (here / "README.md").read_text(encoding="utf-8")
except FileNotFoundError:
    long_description = """
    Enterprise Job Orchestrator

    A comprehensive, enterprise-grade job orchestration system designed for processing
    massive datasets (80+ million records) with distributed workers, fault tolerance,
    and comprehensive monitoring.
    """

setup(
    name="enterprise-job-orchestrator",
    version="1.0.0",
    description="Enterprise-grade job orchestration system for massive data processing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/enterprise/job-orchestrator",
    author="Enterprise Job Orchestrator Team",
    author_email="dev-team@enterprise.com",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Systems Administration",
    ],
    keywords="job orchestration, distributed computing, data processing, enterprise, async",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "asyncpg>=0.27.0",
        "click>=8.0.0",
        "psutil>=5.8.0",
        "typing-extensions>=4.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "black>=22.0.0",
            "isort>=5.10.0",
            "mypy>=1.0.0",
            "coverage>=6.0.0",
            "flake8>=5.0.0",
        ],
        "monitoring": [
            "prometheus-client>=0.14.0",
            "grafana-api>=1.0.3",
        ],
        "web": [
            "fastapi>=0.95.0",
            "uvicorn>=0.20.0",
            "jinja2>=3.1.0",
        ],
        "redis": [
            "redis>=4.0.0",
            "aioredis>=2.0.0",
        ],
        "all": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "black>=22.0.0",
            "isort>=5.10.0",
            "mypy>=1.0.0",
            "coverage>=6.0.0",
            "flake8>=5.0.0",
            "prometheus-client>=0.14.0",
            "grafana-api>=1.0.3",
            "fastapi>=0.95.0",
            "uvicorn>=0.20.0",
            "jinja2>=3.1.0",
            "redis>=4.0.0",
            "aioredis>=2.0.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "enterprise-job-orchestrator=enterprise_job_orchestrator.cli.main:main",
            "ejo=enterprise_job_orchestrator.cli.main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "enterprise_job_orchestrator": [
            "sql/*.sql",
            "templates/*.jinja2",
            "config/*.yaml",
            "docs/*.md",
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/enterprise/job-orchestrator/issues",
        "Source": "https://github.com/enterprise/job-orchestrator",
        "Documentation": "https://job-orchestrator.enterprise.com/docs",
    },
)