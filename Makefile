.PHONY: help install test lint format clean package

help:
	@echo "Available commands:"
	@echo "  make install    - Install dependencies"
	@echo "  make test       - Run tests"
	@echo "  make lint       - Run linters"
	@echo "  make format     - Format code with black"
	@echo "  make clean      - Clean build artifacts"
	@echo "  make package    - Package code for AWS Console deployment"
	@echo ""
	@echo "NOTE: This project uses manual AWS Console deployment."
	@echo "See DEPLOYMENT_GUIDE.md for complete step-by-step instructions."

install:
	pip install -r requirements.txt
	pip install -e .

test:
	pytest tests/ -v --cov=. --cov-report=html --cov-report=term

lint:
	flake8 schema_engine/ data_quality/ glue_jobs/ lambda/ config/
	pylint schema_engine/ data_quality/ glue_jobs/ lambda/ config/
	mypy schema_engine/ data_quality/ glue_jobs/ lambda/ config/

format:
	black schema_engine/ data_quality/ glue_jobs/ lambda/ config/ tests/

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

package:
	@echo "Packaging code for AWS Console deployment..."
	mkdir -p dist
	pip install -r requirements.txt -t /tmp/python-packages
	cd /tmp/python-packages && zip -r $(PWD)/dist/dependencies.zip . && cd -
	cd schema_engine && zip -r ../dist/dependencies.zip . && cd ..
	cd data_quality && zip -r ../dist/dependencies.zip . && cd ..
	cd config && zip -r ../dist/dependencies.zip . && cd ..
	cd lambda && zip lambda_function.zip trigger_pipeline.py && cd ..
	@echo "✓ Code packaged successfully!"
	@echo ""
	@echo "Next steps:"
	@echo "1. Upload dist/dependencies.zip to S3 via AWS Console"
	@echo "2. Upload glue_jobs/orders_etl_job.py to S3 via AWS Console"
	@echo "3. Upload lambda/lambda_function.zip via Lambda Console"
	@echo ""
	@echo "See DEPLOYMENT_GUIDE.md for detailed instructions."
