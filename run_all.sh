#!/bin/bash

# Run data scraper script
spark-submit /app/src/data_scraper.py

# Run data processing script
spark-submit /app/src/data_processing.py

# Run modeling script
spark-submit /app/src/modelling.py

# Run final analysis script
spark-submit /app/src/final_analysis.py