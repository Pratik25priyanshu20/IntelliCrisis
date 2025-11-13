🌍 IntelliCrisis: Real-Time Disaster News & Event Analyzer

IntelliCrisis is a full-stack, real-time AI-powered analytics system designed to connect breaking news articles with real-world disaster events (wildfires and landslides). It uses modern data engineering and AI tools to ingest, enrich, and visualize disaster-related data from multiple sources.

⸻

🧩 Problem Statement

Natural disasters are reported across numerous platforms — but there’s often a disconnect between news articles, real-time events, and public awareness. The system aims to:
	•	Bridge the gap between disaster news and verified events
	•	Provide semantic matching between articles and real incidents
	•	Offer a clean, real-time dashboard to explore trends




IntelliCrisis:
	•	Ingests real-time news from APIs (NewsAPI, Mediastack, GNews, Reddit, Google RSS)
	•	Consumes NASA FIRMS wildfire data and structured landslide records
	•	Uses Kafka to stream data into a unified pipeline
	•	Semantically links news to real events using Sentence-BERT
	•	Orchestrated using Dockerized Apache Airflow
	•	Visualized using a single-file Streamlit dashboard
	•	Supports storage in MongoDB or Databricks Delta Lake




# 1. Clone the repo
git clone https://github.com/Pratik25priyanshu20/IntelliCrisis.git
cd IntelliCrisis

# 2. Ensure .env contains your keys
cp .env.example .env
# Edit and add keys for:
# - NEWS_API_KEY, GNEWS_API_KEY, REDDIT_CLIENT_ID, etc.

# 3. Start everything via Docker Compose
docker-compose up -d --build

# 4. Upload initial data into MongoDB container (only once)
docker cp path/to/news.json intellicrisis-mongo:/news.json
docker cp path/to/fires.json intellicrisis-mongo:/fires.json
docker cp path/to/slides.json intellicrisis-mongo:/slides.json
docker cp path/to/gnews_articles.json intellicrisis-mongo:/gnews_articles.json

# Inside container:
docker exec -it intellicrisis-mongo bash
mongoimport --db disaster_db --collection news_articles --file /news.json --jsonArray
mongoimport --db disaster_db --collection wildfires --file /fires.json --jsonArray
mongoimport --db disaster_db --collection landslides --file /slides.json --jsonArray
mongoimport --db disaster_db --collection gnews_articles --file /gnews_articles.json --jsonArray
exit

# 5. Open Airflow UI to trigger DAGs
http://localhost:8081

# 6. Run Semantic Scoring DAG
Trigger `semantic_scorer_dag` manually

# 7. Open the Dashboard
http://localhost:8501

