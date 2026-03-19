"""
Setup script for AWS Data Lake Schema Evolution Framework.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="aws-schema-evolution-framework",
    version="1.0.0",
    author="Data Engineering Team",
    description="AWS Data Lake Schema Evolution and Data Quality Framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/aws-schema-evolution-framework",
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=[
        "boto3>=1.34.0",
        "pyspark>=3.4.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=7.0.0",
            "mypy>=1.8.0",
        ],
    },
)
