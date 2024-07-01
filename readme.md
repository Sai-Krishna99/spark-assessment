### Instructions to run this:

#### Step 1 Clone the repo
```bash
git clone https://github.com/Sai-Krishna99/spark-assessment.git
```

### Step 2 Pull the docker images from dockerhub
```bash
docker pull saikrishnajasti/jira-spark-app:spark-master
docker pull saikrishnajasti/jira-spark-app:spark-worker
docker pull saikrishnajasti/jira-spark-app:spark-submit
```

### Step 3 Run the docker compose
```
docker-compose up 
```

### Step 4 Open the outputs of the questions will be saved under logs directory under the name ```final_analysis.log```

### Also check the notebooks within the ```src``` for more details

#### Directory structure:

```markdown
/project-directory
  ├── data/
  │   ├── cleaned_data.parquet
  │   ├── predicted_datav0.parquet
  │   ├── data.json
  ├── src/
  │   ├── analysis.ipynb
  │   ├── data_processing.ipynb
  │   ├── data_processing.py
  │   ├── data_scraper.py
  │   ├── final_analysis.py
  │   ├── modeling.ipynb
  │   ├── modelling.py
  │   ├── view_json_structure.ipynb
  ├── tests/
  │   ├── __pycache__
  │   ├── mlen_test.py
  ├── venv/
  ├── .dockerignore
  ├── docker-compose.yml
  ├── Dockerfile
  ├── requirements.txt
  ├── run_all.sh
```
