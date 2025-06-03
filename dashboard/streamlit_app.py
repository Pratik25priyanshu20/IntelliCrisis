import os
import pandas as pd
import streamlit as st
import folium
from pymongo import MongoClient
from dotenv import load_dotenv
from folium.plugins import MarkerCluster, HeatMap
from streamlit_folium import st_folium

# Load environment variables
load_dotenv(dotenv_path="config/.env")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# MongoDB connection
client = MongoClient(MONGO_URI)
db = client["disaster_db"]
articles = list(db["news_articles"].find({"linked_fire_id": {"$exists": True}}))
fires = list(db["wildfires"].find())

# Streamlit page config
st.set_page_config(layout="wide")
st.title("🧠 Disaster News & Wildfire Impact Analyzer")

# Prepare DataFrame
article_df = pd.DataFrame([
    {
        "Title": a.get("title"),
        "Source": a.get("source"),
        "Published": a.get("publishedAt"),
        "Region": a.get("query_region"),
        "URL": a.get("url"),
        "Relevance": a.get("relevance_score"),
        "Semantic": a.get("semantic_score"),
        "Match": a.get("match_score"),
        "Lat": a.get("query_lat"),
        "Lon": a.get("query_lon"),
        "Entities": ", ".join([e["text"] for e in a.get("entities", [])])
    }
    for a in articles
])

# Sidebar filters
with st.sidebar:
    st.header("🔍 Filters")
    region = st.multiselect("Select Region", options=sorted(article_df["Region"].unique()), default=None)
    score_thresh = st.slider("Minimum Relevance Score", 0.0, 1.0, 0.0, 0.1)

# Filter logic
if region:
    article_df = article_df[article_df["Region"].isin(region)]
article_df = article_df[article_df["Relevance"] >= score_thresh]

# Show table
st.subheader("📰 News Articles")
st.dataframe(article_df, use_container_width=True)

# Map
st.subheader("🗺️ Fire and News Map")
map_center = [51.0, 10.0]
fire_map = folium.Map(location=map_center, zoom_start=6)

# Add fires to map
for fire in fires:
    folium.CircleMarker(
        location=[fire["latitude"], fire["longitude"]],
        radius=4,
        color="red",
        fill=True,
        fill_opacity=0.7,
        popup=f"🔥 Fire: {fire['acq_date']}"
    ).add_to(fire_map)

# Cluster for articles
cluster = MarkerCluster().add_to(fire_map)
for _, row in article_df.iterrows():
    if pd.notnull(row["Lat"]) and pd.notnull(row["Lon"]):
        popup_text = f"<b>{row['Title']}</b><br>{row['Published']}<br><a href='{row['URL']}' target='_blank'>Read more</a>"
        folium.Marker(
            location=[row["Lat"], row["Lon"]],
            popup=popup_text,
            icon=folium.Icon(color="blue", icon="info-sign")
        ).add_to(cluster)

# Optional heatmap overlay
st.markdown("---")
if st.checkbox("🌡️ Show Fire Density Heatmap"):
    heat_data = [[fire["latitude"], fire["longitude"]] for fire in fires]
    HeatMap(heat_data).add_to(fire_map)

st_data = st_folium(fire_map, width=1400, height=600)
