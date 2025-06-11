'''
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timezone
from pymongo import MongoClient, errors
import os # Import os for environment variables


# --- Configuration ---
# Set Streamlit page configuration for a wide layout and title
st.set_page_config(
    page_title="Intellicrisis : Global Disaster Analysis", # CHANGED: Page title in browser tab
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- MongoDB Connection ---
@st.cache_resource # Cache the MongoDB connection to prevent reconnection on every rerun
def get_mongo_client():
    """Establishes and returns a MongoDB client connection."""
    try:
        # Use an environment variable or direct string for the MongoDB URI
        # Make sure your Docker container is running and port 27017 is exposed
        mongo_uri = os.getenv("MONGO_URI", "mongodb://intellicrisis-mongo:27017/") 
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        # The ping command is cheap and does not require auth.
        client.admin.command('ping')
        # REMOVED: st.success("Successfully connected to MongoDB! 🚀")
        return client
    except errors.ServerSelectionTimeoutError as err:
        st.error(f"Could not connect to MongoDB: {err}. Please ensure your MongoDB container (intellicrisis-mongo) is running and accessible on {mongo_uri}")
        st.stop() # Stop the app if connection fails
    except Exception as e:
        st.error(f"An unexpected error occurred while connecting to MongoDB: {e}")
        st.stop()

client = get_mongo_client()
db = client["disaster_db"] # Assuming your database is named 'disaster_db'


# --- Custom CSS for UI/UX enhancements ---
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');

    html, body, [class*="st-"] {
        font-family: 'Inter', sans-serif;
        color: #333333; /* Darker text for readability */
    }

    /* General container styling */
    .st-emotion-cache-1cypcdb { /* Target the main block container */
        border-radius: 12px; /* Rounded corners for content blocks */
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1); /* Subtle shadow */
        padding: 20px;
        margin-bottom: 20px;
        background-color: #ffffff;
    }
    /* Specific for metric boxes (which use a slightly different class sometimes) */
    .st-emotion-cache-nahz7x { /* This might be for the button wrapper */
        border-radius: 8px; /* For recent activity buttons */
    }


    /* Sidebar styling */
    .st-emotion-cache-16txt4y { /* Sidebar background */
        background-color: #f0f2f6; /* Lighter grey for sidebar */
        border-right: 1px solid #e0e0e0;
        border-top-right-radius: 12px;
        border-bottom-right-radius: 12px;
        box-shadow: 2px 0 5px rgba(0,0,0,0.05);
    }
    .st-emotion-cache-1wqx_4d { /* Sidebar title */
        color: #2c3e50;
        font-weight: 700;
    }

    /* Header and subheader styling */
    h1, h2, h3, h4, h5, h6 {
        color: #2c3e50; /* Darker blue for headings */
        font-weight: 600;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }

    /* Buttons styling */
    .st-emotion-cache-nahz7x > button { /* Target Streamlit button itself inside the container */
        border-radius: 8px;
        border: 1px solid #3498db;
        background-color: #3498db;
        color: white;
        padding: 10px 15px;
        font-weight: 500;
        transition: background-color 0.3s, transform 0.3s;
    }
    .st-emotion-cache-nahz7x > button:hover {
        background-color: #2980b9;
        border-color: #2980b9;
        transform: translateY(-2px);
    }
    
    /* Specific styling for the clickable metric-like buttons in Overview */
    .st-emotion-cache-nahz7x .stButton > button {
        background-color: #4CAF50; /* Green for recent activity buttons */
        border-color: #4CAF50;
        font-size: 1.1em;
        font-weight: 600;
        color: white;
    }
    .st-emotion-cache-nahz7x .stButton > button:hover {
        background-color: #45a049;
        border-color: #45a049;
    }


    /* Metric boxes */
    .st-emotion-cache-l9bibm { /* Target st.metric label */
        font-size: 1.1em;
        font-weight: 600;
        color: #555555;
    }
    .st-emotion-cache-12ttj6z { /* Target st.metric value */
        font-size: 2.2em;
        font-weight: 700;
        color: #1a1a1a;
    }

    /* Info boxes */
    .st-emotion-cache-1uj25v8 { /* Target st.info block */
        border-radius: 8px;
        background-color: #e8f5e9; /* Light green for info */
        border-left: 5px solid #4CAF50;
        padding: 15px;
        margin-bottom: 15px;
    }
    .st-emotion-cache-1uj25v8 p {
        color: #333333;
    }

    /* Warning boxes */
    .st-emotion-cache-176lqf0 { /* Target st.warning block */
        border-radius: 8px;
        background-color: #fff3e0; /* Light orange for warning */
        border-left: 5px solid #ff9800;
        padding: 15px;
        margin-bottom: 15px;
    }
    .st-emotion-cache-176lqf0 p {
        color: #333333;
    }

    /* Expander styling for news articles */
    .st-emotion-cache-r42gji { /* Target expander header */
        border-radius: 8px;
        background-color: #f8f9fa; /* Light grey for expander headers */
        padding: 10px 15px;
        margin-bottom: 5px;
        border: 1px solid #e0e0e0;
    }
    .st-emotion-cache-r42gji:hover {
        background-color: #e9ecef;
    }
    .st-emotion-cache-1w0rc8c { /* Target expander content */
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        border-top: none;
        border-bottom-left-radius: 8px;
        border-bottom-right-radius: 8px;
        padding: 15px;
        margin-bottom: 15px;
    }
    </style>
    """,
    unsafe_allow_html=True
)


# --- Data Loading Functions (from MongoDB) ---
@st.cache_data(ttl=3600) # Cache data for 1 hour to avoid constant DB queries
def load_data_from_mongo(collection_name, date_field=None):
    """
    Loads data from a specified MongoDB collection into a Pandas DataFrame.
    Converts a specified date field to datetime objects if provided.
    """
    try:
        # Fetch all documents from the collection
        cursor = db[collection_name].find({})
        df = pd.DataFrame(list(cursor))
        
        # Convert _id to string for easier display if needed
        if '_id' in df.columns:
            df['_id'] = df['_id'].astype(str)

        if date_field and date_field in df.columns:
            # Convert to datetime, coercing errors to NaT and ensuring UTC timezone
            df[date_field] = pd.to_datetime(df[date_field], errors='coerce', utc=True)
            # Remove rows where date_field is NaT after conversion
            df.dropna(subset=[date_field], inplace=True)
            
        return df
    except Exception as e:
        st.error(f"Error loading data from MongoDB collection '{collection_name}': {e}")
        return pd.DataFrame()

# Load all datasets with a spinner for better UX
with st.spinner('Connecting to MongoDB and loading data... This might take a moment.'):
    df_wildfires = load_data_from_mongo("wildfires", date_field='acq_date')
    df_landslides = load_data_from_mongo("landslides", date_field='date')
    # Assuming 'news_articles' and 'gnews_articles' are your collections for news
    df_news_articles = load_data_from_mongo("news_articles", date_field='publishedAt')
    df_gnews_articles = load_data_from_mongo("gnews_articles", date_field='publishedAt') # Assuming this is the GNews collection name


# --- Session State Initialization ---
# Initialize session state variables if they don't exist
if 'page_selection' not in st.session_state:
    st.session_state.page_selection = "Overview"
if 'wildfires_filter_dates' not in st.session_state:
    st.session_state.wildfires_filter_dates = None # Stores (start_datetime, end_datetime) for slider
if 'landslides_filter_dates' not in st.session_state:
    st.session_state.landslides_filter_dates = None
if 'news_filter_dates' not in st.session_state:
    st.session_state.news_filter_dates = None


# --- Sidebar Navigation ---
with st.sidebar:
    st.title("Navigation")
    st.markdown("---") # Separator for better aesthetics

    # Use a common key for the radio button to ensure it works smoothly with st.session_state
    current_page_from_sidebar = st.radio(
        "Go to",
        ["Overview", "Wildfires Analysis", "Landslides Analysis", "News & Trends"],
        index=["Overview", "Wildfires Analysis", "Landslides Analysis", "News & Trends"].index(st.session_state.page_selection),
        key="sidebar_radio" # Add a key
    )

    # Update page selection if sidebar radio changes, and reset specific filters
    if current_page_from_sidebar != st.session_state.page_selection:
        st.session_state.page_selection = current_page_from_sidebar
        # Reset specific filters if user navigates via sidebar to see full data range
        st.session_state.wildfires_filter_dates = None
        st.session_state.landslides_filter_dates = None
        st.session_state.news_filter_dates = None
        st.rerun() # Rerun to apply page change immediately

    st.markdown("---") # Separator


# --- Common Dashboard Elements ---
# CHANGED: Main title
st.title("Intellicrisis : Global Disaster Analysis")
st.markdown("A comprehensive look into natural disaster data from various sources.")


# --- Page Content Based on Selection ---


if st.session_state.page_selection == "Overview":
    st.header("Dashboard Overview")
    st.write("This dashboard provides insights into global wildfires, landslides, and related news articles. Use the sidebar to navigate through different analysis sections, or click the recent activity metrics below to jump directly to filtered views.")


    st.markdown("---") # Visual separator


    # Metrics with an enhanced layout
    col1, col2, col3 = st.columns(3)


    with col1:
        st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Total Wildfire Records</h3><p class='st-emotion-cache-12ttj6z'>{len(df_wildfires) if not df_wildfires.empty else 0}</p></div>", unsafe_allow_html=True)
    with col2:
        st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Total Landslide Records</h3><p class='st-emotion-cache-12ttj6z'>{len(df_landslides) if not df_landslides.empty else 0}</p></div>", unsafe_allow_html=True)
    with col3:
        total_news_articles_count = (len(df_news_articles) if not df_news_articles.empty else 0) + \
                                    (len(df_gnews_articles) if not df_gnews_articles.empty else 0)
        st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Total News Articles</h3><p class='st-emotion-cache-12ttj6z'>{total_news_articles_count}</p></div>", unsafe_allow_html=True)


    st.subheader("Recent Activity (Last 30 days) - Click to explore!")
    st.markdown("---") # Visual separator
    
    # Define the 30-day range as timezone-aware Pandas Timestamps
    # Set end_date to end of today to include all data for current day
    end_30_days_ts = pd.Timestamp.now(tz='UTC').normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    start_30_days_ts = end_30_days_ts - pd.Timedelta(days=30)


    # Convert to Python datetime objects for storing in session state and st.slider
    start_30_days_dt = start_30_days_ts.to_pydatetime()
    end_30_days_dt = end_30_days_ts.to_pydatetime()


    recent_col1, recent_col2, recent_col3 = st.columns(3)


    with recent_col1:
        if not df_wildfires.empty:
            # Filter using timezone-aware Timestamps
            wildfires_recent_count = df_wildfires[
                (df_wildfires['acq_date'] >= start_30_days_ts) &
                (df_wildfires['acq_date'] <= end_30_days_ts)
            ].shape[0]
            
            # Make it a clickable button with updated styling
            if st.button(f"🔥 **{wildfires_recent_count}** Recent Wildfires", key="recent_wildfires_btn", use_container_width=True):
                st.session_state.page_selection = "Wildfires Analysis"
                st.session_state.wildfires_filter_dates = (start_30_days_dt, end_30_days_dt)
                st.rerun() # Rerun to apply the new page and filter
        else:
            st.info("No wildfire data.")


    with recent_col2:
        if not df_landslides.empty:
            landslides_recent_count = df_landslides[
                (df_landslides['date'] >= start_30_days_ts) &
                (df_landslides['date'] <= end_30_days_ts)
            ].shape[0]
            if st.button(f"⛰️ **{landslides_recent_count}** Recent Landslides", key="recent_landslides_btn", use_container_width=True):
                st.session_state.page_selection = "Landslides Analysis"
                st.session_state.landslides_filter_dates = (start_30_days_dt, end_30_days_dt)
                st.rerun()
        else:
            st.info("No landslide data.")


    with recent_col3:
        # Use df_gnews_articles for the count as it's typically more recent/dynamic
        # And check for 'publishedAt' as it's the date field
        if not df_gnews_articles.empty and 'publishedAt' in df_gnews_articles.columns:
            gnews_recent_count = df_gnews_articles[
                (df_gnews_articles['publishedAt'] >= start_30_days_ts) &
                (df_gnews_articles['publishedAt'] <= end_30_days_ts)
            ].shape[0]
            if st.button(f"📰 **{gnews_recent_count}** Recent News Articles", key="recent_gnews_btn", use_container_width=True):
                st.session_state.page_selection = "News & Trends"
                st.session_state.news_filter_dates = (start_30_days_dt, end_30_days_dt)
                st.rerun()
        else:
            st.info("No GNews articles or 'publishedAt' field not found.")

    st.subheader("Global Disaster Map (Overview)")
    st.markdown("Visualize the distribution of recent wildfires and landslides globally.")
    st.info("💡 **Clickable Map Markers**: Hover over points for details. You can zoom and pan the map to explore specific regions.")

    # Prepare data for combined map
    map_data = pd.DataFrame()
    if not df_wildfires.empty:
        # Ensure 'latitude', 'longitude' exist and are numeric
        df_wildfires_map = df_wildfires[['latitude', 'longitude', 'acq_date', 'brightness', 'country']].copy()
        df_wildfires_map['type'] = 'Wildfire'
        df_wildfires_map['description'] = df_wildfires_map.apply(lambda row: f"Wildfire in {row['country']} on {row['acq_date'].strftime('%Y-%m-%d')} (Brightness: {row['brightness']:.2f}K)", axis=1)
        map_data = pd.concat([map_data, df_wildfires_map.rename(columns={'acq_date': 'date'})], ignore_index=True)

    if not df_landslides.empty:
        # Ensure 'latitude', 'longitude' exist and are numeric
        df_landslides_map = df_landslides[['latitude', 'longitude', 'date', 'location', 'description', 'type']].copy()
        df_landslides_map['type'] = df_landslides_map['type'].fillna('Landslide') # Ensure 'type' exists
        df_landslides_map['description'] = df_landslides_map.apply(lambda row: f"Landslide in {row['location']} on {row['date'].strftime('%Y-%m-%d')}. Type: {row['type']}", axis=1)
        map_data = pd.concat([map_data, df_landslides_map], ignore_index=True)

    if not map_data.empty:
        # Convert lat/lon to numeric, coercing errors
        map_data['latitude'] = pd.to_numeric(map_data['latitude'], errors='coerce')
        map_data['longitude'] = pd.to_numeric(map_data['longitude'], errors='coerce')
        map_data.dropna(subset=['latitude', 'longitude'], inplace=True) # Drop rows with invalid coordinates

        # Filter for recent activity for the overview map if you want a lighter map
        # For a full overview, you might want to remove this filter or make it optional
        # Here, I'll use the 'recent_activity' filter for this overview map
        map_data_recent = map_data[
            (map_data['date'] >= start_30_days_ts) &
            (map_data['date'] <= end_30_days_ts)
        ].copy() # Filter for recent activity, use .copy() to prevent SettingWithCopyWarning

        if not map_data_recent.empty:
            fig_overview_map = px.scatter_mapbox(
                map_data_recent,
                lat="latitude",
                lon="longitude",
                color="type", # Color by disaster type
                hover_name="description", # Use the combined description
                hover_data={"date": "|%Y-%m-%d", "type": True},
                color_discrete_map={'Wildfire': 'red', 'Landslide': 'darkblue'}, # Custom colors
                zoom=1, # Broader initial zoom
                height=550,
                mapbox_style="carto-positron",
                title="Recent Global Disaster Incidents (Last 30 Days)"
            )
            fig_overview_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_overview_map, use_container_width=True)
        else:
            st.info("No recent disaster data available for the overview map in the last 30 days.")
    else:
        st.warning("No wildfire or landslide data found to display on the map.")


elif st.session_state.page_selection == "Wildfires Analysis":
    st.header("🔥 Wildfires Analysis")
    st.markdown("Explore global wildfire data, including locations, trends, and key metrics. Use the date slider to focus on specific periods.")
    st.markdown("---")


    if df_wildfires.empty:
        st.warning("No wildfire data available to display. Please check the `wildfires` collection in MongoDB.")
    else:
        df_wildfires_copy = df_wildfires.copy() # Use .copy() to avoid SettingWithCopyWarning


        # Drop NaT values before finding min/max dates to avoid errors
        df_wildfires_clean_dates = df_wildfires_copy.dropna(subset=['acq_date'])


        if df_wildfires_clean_dates.empty:
            st.info("No valid dates found in wildfire data for date range selection.")
            filtered_df_wildfires = pd.DataFrame() # No data to filter
        else:
            min_date_w_ts = df_wildfires_clean_dates['acq_date'].min()
            max_date_w_ts = df_wildfires_clean_dates['acq_date'].max()


            # Handle case where min and max dates are the same (e.g., only one day of data)
            if min_date_w_ts == max_date_w_ts:
                # Extend max_date by 1 day to ensure a valid slider range
                max_date_w_ts = min_date_w_ts + pd.Timedelta(days=1)


            # Convert Timestamps to Python datetime objects for slider range
            min_date_w_dt = min_date_w_ts.to_pydatetime()
            max_date_w_dt = max_date_w_ts.to_pydatetime()


            # Set initial value for slider from session state or default to full range
            default_value_w = st.session_state.wildfires_filter_dates if st.session_state.wildfires_filter_dates else (min_date_w_dt, max_date_w_dt)


            date_range_w = st.slider(
                "Select Date Range for Wildfires",
                min_value=min_date_w_dt,
                max_value=max_date_w_dt,
                value=default_value_w,
                format="YYYY-MM-DD",
                key="wildfires_date_slider" # Add a key for the slider
            )
            
            # Update session state when slider changes (only if it's different from initial filter)
            if date_range_w != st.session_state.wildfires_filter_dates:
                st.session_state.wildfires_filter_dates = date_range_w


            # Convert slider output (timezone-aware datetime) to UTC Timestamp correctly for filtering
            start_date_filter_w = pd.to_datetime(date_range_w[0], utc=True)
            end_date_filter_w = pd.to_datetime(date_range_w[1], utc=True)


            filtered_df_wildfires = df_wildfires_copy[
                (df_wildfires_copy['acq_date'] >= start_date_filter_w) &
                (df_wildfires_copy['acq_date'] <= end_date_filter_w)
            ]
        
        st.markdown("---") # Visual separator


        # Summary Metrics for filtered wildfires
        if not filtered_df_wildfires.empty:
            col_metrics1, col_metrics2, col_metrics3 = st.columns(3)
            with col_metrics1:
                st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Total Wildfires (Filtered)</h3><p class='st-emotion-cache-12ttj6z'>{len(filtered_df_wildfires)}</p></div>", unsafe_allow_html=True)
            with col_metrics2:
                avg_brightness = filtered_df_wildfires['brightness'].mean()
                st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Avg. Brightness (K)</h3><p class='st-emotion-cache-12ttj6z'>{avg_brightness:.2f}</p></div>", unsafe_allow_html=True)
            with col_metrics3:
                # Fix: Handle formatting when max_frp might be None or NaN (after isnull().all())
                max_frp_val = filtered_df_wildfires['frp'].max() if 'frp' in filtered_df_wildfires.columns and not filtered_df_wildfires['frp'].isnull().all() else None
                
                # Format max_frp_val only if it's a number, otherwise display 'N/A'
                display_max_frp = f"{max_frp_val:.2f}" if isinstance(max_frp_val, (int, float)) else "N/A"
                st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Max FRP (MW)</h3><p class='st-emotion-cache-12ttj6z'>{display_max_frp}</p></div>", unsafe_allow_html=True)
        else:
            st.info("No wildfire data for the selected date range to calculate metrics.")


        st.subheader("Wildfire Locations Map")
        st.markdown("Explore wildfire hotspots on the interactive map. The size and color of the marker indicate brightness.")
        # Added explanation for brightness as a styled info box
        st.info("💡 **Understanding Brightness:** The 'Brightness' values on the map represent the fire radiative temperature measured in **Kelvin (K)**. This measurement is derived from thermal infrared data captured by satellites. Higher brightness values generally indicate hotter and more intense fires, and these correspond to brighter colors and larger markers on the map.")
        if not filtered_df_wildfires.empty:
            fig_map_w = px.scatter_mapbox(
                filtered_df_wildfires,
                lat="latitude",
                lon="longitude",
                color="brightness", # Color by brightness
                size="brightness",  # Size by brightness
                hover_name="country",
                hover_data={"acq_date": "|%Y-%m-%d", "confidence": True, "frp": True, "brightness": ":.2f"}, # Format date
                color_continuous_scale=px.colors.sequential.Inferno,
                zoom=2,
                height=500,
                mapbox_style="carto-positron", # A clean, neutral map style
                title="Interactive Wildfire Map" # Add title to map
            )
            fig_map_w.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_map_w, use_container_width=True)
        else:
            st.info("No wildfire data for the selected date range to display on the map.")


        st.markdown("---") # Visual separator
        col_w1, col_w2 = st.columns(2)


        with col_w1:
            st.subheader("Wildfires Over Time")
            st.markdown("See the trend of wildfire occurrences over the selected period.")
            if not filtered_df_wildfires.empty:
                wildfires_monthly = filtered_df_wildfires.set_index('acq_date').resample('M').size().reset_index(name='Count')
                fig_time_w = px.line(
                    wildfires_monthly,
                    x='acq_date',
                    y='Count',
                    title='Wildfires Count Monthly',
                    labels={'acq_date': 'Date', 'Count': 'Number of Wildfires'},
                    markers=True # Add markers for clarity
                )
                fig_time_w.update_xaxes(dtick="M1", tickformat="%b\n%Y")
                st.plotly_chart(fig_time_w, use_container_width=True)
            else:
                st.info("No wildfire time series data for the selected date range.")


        with col_w2:
            st.subheader("Top Countries by Wildfire Count")
            st.markdown("Identify countries with the highest number of reported wildfires.")
            if not filtered_df_wildfires.empty:
                wildfires_by_country = filtered_df_wildfires['country'].value_counts().reset_index()
                wildfires_by_country.columns = ['Country', 'Count']
                fig_bar_w_country = px.bar(
                    wildfires_by_country.head(10), # Show top 10
                    x='Count',
                    y='Country',
                    orientation='h',
                    title='Top 10 Countries with Wildfires',
                    labels={'Count': 'Number of Wildfires', 'Country': 'Country Code'},
                    color_discrete_sequence=px.colors.sequential.Plasma_r # Reverse color scale for better contrast
                )
                fig_bar_w_country.update_layout(yaxis={'categoryorder':'total ascending'}) # Order bars by count
                st.plotly_chart(fig_bar_w_country, use_container_width=True)
            else:
                st.info("No wildfire country data for the selected date range.")


        st.subheader("Brightness vs. FRP (Fire Radiative Power)")
        st.markdown("Analyze the relationship between fire brightness and radiative power, with confidence level. FRP (Fire Radiative Power) quantifies the rate of energy released by the fire.")
        if not filtered_df_wildfires.empty and 'frp' in filtered_df_wildfires.columns and 'confidence' in filtered_df_wildfires.columns:
            fig_scatter_w = px.scatter(
                filtered_df_wildfires,
                x='frp',
                y='brightness',
                color='confidence', # Color points by confidence
                hover_name='country',
                title='Wildfire Brightness vs. FRP by Confidence',
                labels={'frp': 'Fire Radiative Power (MW)', 'brightness': 'Brightness (Kelvin)'}
            )
            st.plotly_chart(fig_scatter_w, use_container_width=True)
        else:
            st.info("Relevant data (FRP or confidence) not available for scatter plot for the selected date range.")


elif st.session_state.page_selection == "Landslides Analysis":
    st.header("⛰️ Landslides Analysis")
    st.markdown("Visualize global landslide incidents and their trends over time. Filter data using the slider to explore specific periods.")
    st.markdown("---")


    if df_landslides.empty:
        st.warning("No landslide data available to display. Please check the `landslides` collection in MongoDB.")
    else:
        df_landslides_copy = df_landslides.copy() # Use .copy()


        df_landslides_clean_dates = df_landslides_copy.dropna(subset=['date'])


        if df_landslides_clean_dates.empty:
            st.info("No valid dates found in landslide data for date range selection.")
            filtered_df_landslides = pd.DataFrame()
        else:
            min_date_l_ts = df_landslides_clean_dates['date'].min()
            max_date_l_ts = df_landslides_clean_dates['date'].max()


            # Handle case where min and max dates are the same
            if min_date_l_ts == max_date_l_ts:
                max_date_l_ts = min_date_l_ts + pd.Timedelta(days=1)


            min_date_l_dt = min_date_l_ts.to_pydatetime()
            max_date_l_dt = max_date_l_ts.to_pydatetime()


            # Set initial value for slider from session state or default to full range
            default_value_l = st.session_state.landslides_filter_dates if st.session_state.landslides_filter_dates else (min_date_l_dt, max_date_l_dt)


            date_range_l = st.slider(
                "Select Date Range for Landslides",
                min_value=min_date_l_dt,
                max_value=max_date_l_dt,
                value=default_value_l,
                format="YYYY-MM-DD",
                key="landslides_date_slider" # Add a key
            )
            
            # Update session state when slider changes
            if date_range_l != st.session_state.landslides_filter_dates:
                st.session_state.landslides_filter_dates = date_range_l


            start_date_filter_l = pd.to_datetime(date_range_l[0], utc=True)
            end_date_filter_l = pd.to_datetime(date_range_l[1], utc=True)


            filtered_df_landslides = df_landslides_copy[
                (df_landslides_copy['date'] >= start_date_filter_l) &
                (df_landslides_copy['date'] <= end_date_filter_l)
            ]
        
        st.markdown("---") # Visual separator


        # Summary Metrics for filtered landslides
        if not filtered_df_landslides.empty:
            col_metrics_l, _ = st.columns([0.3, 0.7]) # Use columns for metric layout
            with col_metrics_l:
                st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Total Landslides (Filtered)</h3><p class='st-emotion-cache-12ttj6z'>{len(filtered_df_landslides)}</p></div>", unsafe_allow_html=True)
        else:
            st.info("No landslide data for the selected date range to calculate metrics.")


        st.subheader("Landslide Locations Map")
        st.markdown("View reported landslide incidents on the map. Hover over markers for more details.")
        if not filtered_df_landslides.empty:
            fig_map_l = px.scatter_mapbox(
                filtered_df_landslides,
                lat="latitude",
                lon="longitude",
                color="type", # If 'type' column exists, color by it
                hover_name="location",
                hover_data={"date": "|%Y-%m-%d", "description": True, "source": True},
                zoom=2,
                height=500,
                mapbox_style="carto-positron",
                title="Interactive Landslide Map"
            )
            fig_map_l.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_map_l, use_container_width=True)
        else:
            st.info("No landslide data for the selected date range to display on the map.")


        st.markdown("---") # Visual separator
        col_l1, col_l2 = st.columns(2)


        with col_l1:
            st.subheader("Landslides Over Time")
            st.markdown("Track the frequency of landslides over the selected period.")
            if not filtered_df_landslides.empty:
                landslides_monthly = filtered_df_landslides.set_index('date').resample('M').size().reset_index(name='Count')
                fig_time_l = px.line(
                    landslides_monthly,
                    x='date',
                    y='Count',
                    title='Landslides Count Monthly',
                    labels={'date': 'Date', 'Count': 'Number of Landslides'},
                    markers=True
                )
                fig_time_l.update_xaxes(dtick="M1", tickformat="%b\n%Y")
                st.plotly_chart(fig_time_l, use_container_width=True)
            else:
                st.info("No landslide time series data for the selected date range.")


        with col_l2:
            st.subheader("Top Landslide Locations")
            st.markdown("See which locations have experienced the most landslides.")
            if not filtered_df_landslides.empty and 'location' in filtered_df_landslides.columns:
                # Group by location and get count, handling potential non-string types
                landslides_by_location = filtered_df_landslides['location'].astype(str).value_counts().reset_index()
                landslides_by_location.columns = ['Location', 'Count']
                fig_bar_l_location = px.bar(
                    landslides_by_location.head(10), # Show top 10
                    x='Count',
                    y='Location',
                    orientation='h',
                    title='Top 10 Landslide Locations',
                    labels={'Count': 'Number of Landslides', 'Location': 'Location'},
                    color_discrete_sequence=px.colors.sequential.Mint_r
                )
                fig_bar_l_location.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_bar_l_location, use_container_width=True)
            else:
                st.info("No landslide location data for the selected date range.")


elif st.session_state.page_selection == "News & Trends":
    st.header("📰 News and Trends")
    st.markdown("Explore news articles related to natural disasters, analyze publication trends, and filter content by keywords, countries, and disaster types.")
    st.markdown("---")


    # Combine news datasets
    combined_news_df = pd.DataFrame()
    if not df_news_articles.empty:
        combined_news_df = pd.concat([combined_news_df, df_news_articles])
    if not df_gnews_articles.empty:
        combined_news_df = pd.concat([combined_news_df, df_gnews_articles])


    if combined_news_df.empty:
        st.warning("No news article data available to display. Please check `news_articles` and `gnews_articles` collections in MongoDB.")
    else:
        combined_news_df_copy = combined_news_df.copy() # Use .copy()


        df_news_clean_dates = combined_news_df_copy.dropna(subset=['publishedAt'])


        if df_news_clean_dates.empty:
            st.info("No valid dates found in news data for date range selection.")
            filtered_df_news = pd.DataFrame()
        else:
            min_date_n_ts = df_news_clean_dates['publishedAt'].min()
            max_date_n_ts = df_news_clean_dates['publishedAt'].max()


            # Handle case where min and max dates are the same
            if min_date_n_ts == max_date_n_ts:
                max_date_n_ts = min_date_n_ts + pd.Timedelta(days=1)


            min_date_n_dt = min_date_n_ts.to_pydatetime()
            max_date_n_dt = max_date_n_ts.to_pydatetime()


            # Set initial value for slider from session state or default to full range
            default_value_n = st.session_state.news_filter_dates if st.session_state.news_filter_dates else (min_date_n_dt, max_date_n_dt)


            date_range_n = st.slider(
                "Select Date Range for News Articles",
                min_value=min_date_n_dt,
                max_value=max_date_n_dt,
                value=default_value_n,
                format="YYYY-MM-DD",
                key="news_date_slider" # Add a key
            )
            
            # Update session state when slider changes
            if date_range_n != st.session_state.news_filter_dates:
                st.session_state.news_filter_dates = date_range_n


            start_date_filter_n = pd.to_datetime(date_range_n[0], utc=True)
            end_date_filter_n = pd.to_datetime(date_range_n[1], utc=True)


            filtered_df_news = combined_news_df_copy[
                (combined_news_df_copy['publishedAt'] >= start_date_filter_n) &
                (combined_news_df_copy['publishedAt'] <= end_date_filter_n)
            ]


        st.markdown("---") # Visual separator


        # Summary Metrics for filtered news
        if not filtered_df_news.empty:
            col_metrics_n, _ = st.columns([0.3, 0.7])
            with col_metrics_n:
                st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Total News Articles (Filtered)</h3><p class='st-emotion-cache-12ttj6z'>{len(filtered_df_news)}</p></div>", unsafe_allow_html=True)
        else:
            st.info("No news articles for the selected date range to calculate metrics.")


        st.subheader("News Articles Over Time")
        st.markdown("Observe the publication frequency of disaster-related news articles.")
        if not filtered_df_news.empty:
            news_daily = filtered_df_news.set_index('publishedAt').resample('D').size().reset_index(name='Count')
            fig_time_n = px.line(
                news_daily,
                x='publishedAt',
                y='Count',
                title='News Articles Published Daily',
                labels={'publishedAt': 'Date', 'Count': 'Number of Articles'},
                markers=True
            )
            fig_time_n.update_xaxes(dtick="M1", tickformat="%b\n%Y")
            st.plotly_chart(fig_time_n, use_container_width=True)
        else:
            st.info("No news article time series data for the selected date range.")


        st.markdown("---") # Visual separator
        col_n1, col_n2 = st.columns(2)


        with col_n1:
            st.subheader("Top News Article Sources")
            st.markdown("Discover the most frequent sources reporting on disaster events.")
            if not filtered_df_news.empty and 'source' in filtered_df_news.columns:
                news_by_source = filtered_df_news['source'].value_counts().reset_index()
                news_by_source.columns = ['Source', 'Count']
                fig_bar_n_source = px.bar(
                    news_by_source.head(10),
                    x='Count',
                    y='Source',
                    orientation='h',
                    title='Top 10 News Article Sources',
                    labels={'Count': 'Number of Articles', 'Source': 'News Source'},
                    color_discrete_sequence=px.colors.sequential.Teal_r
                )
                fig_bar_n_source.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_bar_n_source, use_container_width=True)
            else:
                st.info("No news source data for the selected date range.")


        with col_n2:
            st.subheader("Distribution of Disaster Types (from GNews)")
            st.markdown("Understand the breakdown of disaster types mentioned in GNews articles.")
            # Ensure we only use df_gnews_articles for disaster types as requested
            if not df_gnews_articles.empty and 'linked_disaster_type' in df_gnews_articles.columns: # Changed to 'linked_disaster_type'
                # Filter gnews articles by the current date range if needed
                filtered_gnews_for_pie = df_gnews_articles[
                    (df_gnews_articles['publishedAt'] >= start_date_filter_n) &
                    (df_gnews_articles['publishedAt'] <= end_date_filter_n)
                ].copy()

                if not filtered_gnews_for_pie.empty:
                    # Explode list-like 'linked_disaster_type' into separate rows if needed
                    # Assuming 'linked_disaster_type' can be a list or a single string
                    s = filtered_gnews_for_pie['linked_disaster_type'].apply(pd.Series).stack()
                    disaster_type_counts = s.value_counts().reset_index(name='Count')
                    disaster_type_counts.columns = ['Disaster Type', 'Count']

                    if not disaster_type_counts.empty:
                        fig_pie_n = px.pie(
                            disaster_type_counts,
                            values='Count',
                            names='Disaster Type',
                            title='Distribution of News Articles by Linked Disaster Type',
                            hole=0.3 # Add a hole for a donut chart effect
                        )
                        fig_pie_n.update_traces(textposition='inside', textinfo='percent+label')
                        st.plotly_chart(fig_pie_n, use_container_width=True)
                    else:
                        st.info("No linked disaster types found in GNews articles for the selected date range.")
                else:
                    st.info("No GNews articles for the selected date range with disaster types.")
            else:
                st.info("No GNews articles or 'linked_disaster_type' field not found for pie chart.")


        st.markdown("---") # Visual separator
        st.subheader("Filter and Display News Articles")
        st.markdown("Use the filters below to find specific news articles.")


        # News Article Filtering
        col_filter1, col_filter2 = st.columns(2)
        with col_filter1:
            keyword_search = st.text_input("Search by Keyword (in title/description):", "")
        with col_filter2:
            # Get unique disaster types from gnews_articles, handling potential lists
            all_disaster_types = []
            if not df_gnews_articles.empty and 'linked_disaster_type' in df_gnews_articles.columns:
                for types_list in df_gnews_articles['linked_disaster_type'].dropna():
                    if isinstance(types_list, list):
                        all_disaster_types.extend(types_list)
                    else: # Handle case where it might be a single string
                        all_disaster_types.append(types_list)
            
            unique_disaster_types = sorted(list(set(all_disaster_types)))
            
            selected_disaster_type = st.selectbox(
                "Filter by Linked Disaster Type:",
                ["All"] + unique_disaster_types,
                index=0 # Default to "All"
            )


        # Apply keyword filter
        if keyword_search:
            filtered_df_news = filtered_df_news[
                filtered_df_news['title'].fillna('').str.contains(keyword_search, case=False, na=False) |
                filtered_df_news['description'].fillna('').str.contains(keyword_search, case=False, na=False)
            ]

        # Apply linked disaster type filter
        if selected_disaster_type != "All":
            # Filter where 'linked_disaster_type' column contains the selected type (handles lists)
            filtered_df_news = filtered_df_news[
                filtered_df_news['linked_disaster_type'].apply(
                    lambda x: selected_disaster_type in x if isinstance(x, list) else x == selected_disaster_type
                )
            ]


        st.markdown(f"**Displaying {len(filtered_df_news)} filtered articles.**")
        if not filtered_df_news.empty:
            # Sort by publishedAt descending
            filtered_df_news_sorted = filtered_df_news.sort_values(by='publishedAt', ascending=False)
            
            # Display articles in expanders
            for index, row in filtered_df_news_sorted.iterrows():
                # Format date for display
                published_date_str = row['publishedAt'].strftime('%Y-%m-%d %H:%M UTC') if pd.notna(row['publishedAt']) else "N/A"
                
                # Use source and author for header
                source_display = row['source'] if pd.notna(row['source']) else "Unknown Source"
                # MODIFIED LINE: Use .get() to safely access 'author'
                author_val = row.get('author')
                author_display = f" by {author_val}" if pd.notna(author_val) and author_val != 'None' else ""
                
                with st.expander(f"**{row['title']}** - {source_display}{author_display} ({published_date_str})"):
                    if pd.notna(row['description']):
                        st.write(row['description'])
                    if pd.notna(row['url']):
                        st.markdown(f"[Read full article]({row['url']})", unsafe_allow_html=True)
                    
                    # Display linked disaster types if available
                    if 'linked_disaster_type' in row and pd.notna(row['linked_disaster_type']):
                        if isinstance(row['linked_disaster_type'], list):
                            st.markdown(f"**Linked Disaster Types:** {', '.join(row['linked_disaster_type'])}")
                        else:
                            st.markdown(f"**Linked Disaster Type:** {row['linked_disaster_type']}")
                    
                    # Display image if available (ensure 'urlToImage' exists and is a valid URL)
                    if 'urlToImage' in row and pd.notna(row['urlToImage']) and row['urlToImage'].startswith('http'):
                        try:
                            st.image(row['urlToImage'], caption=row['title'], use_column_width=True)
                        except Exception as e:
                            st.warning(f"Could not load image for article '{row['title']}': {e}")
                            st.info("Image URL might be broken or inaccessible.")


        else:
            st.info("No articles match the current filter criteria.")
            
            '''
            
    
    
            
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timezone
from pymongo import MongoClient, errors
import os 
import json 
import requests 

# --- Configuration ---
# Set Streamlit page configuration for a wide layout and title
st.set_page_config(
    page_title="Intellicrisis : Global Disaster Analysis", # Page title in browser tab
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🌍" # Globe icon as requested
)

# --- MongoDB Connection ---
@st.cache_resource # Cache the MongoDB connection to prevent reconnection on every rerun
def get_mongo_client():
    """Establishes and returns a MongoDB client connection."""
    try:
        # Use an environment variable or direct string for the MongoDB URI
        # Make sure your Docker container is running and port 27017 is exposed
        mongo_uri = os.getenv("MONGO_URI", "mongodb://intellicrisis-mongo:27017/") 
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        # The ping command is cheap and does not require auth.
        client.admin.command('ping')
        # REMOVED: st.success("Successfully connected to MongoDB! �")
        return client
    except errors.ServerSelectionTimeoutError as err:
        st.error(f"Could not connect to MongoDB: {err}. Please ensure your MongoDB container (intellicrisis-mongo) is running and accessible on {mongo_uri}")
        st.stop() # Stop the app if connection fails
    except Exception as e:
        st.error(f"An unexpected error occurred while connecting to MongoDB: {e}")
        st.stop()

client = get_mongo_client()
db = client["disaster_db"] # Assuming your database is named 'disaster_db'


# --- Custom CSS for UI/UX enhancements ---
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');

    html, body, [class*="st-"] {
        font-family: 'Inter', sans-serif;
        color: #333333; /* Darker text for readability */
    }

    /* General container styling */
    .st-emotion-cache-1cypcdb { /* Target the main block container */
        border-radius: 12px; /* Rounded corners for content blocks */
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1); /* Subtle shadow */
        padding: 20px;
        margin-bottom: 20px;
        background-color: #ffffff;
    }
    /* Specific for metric boxes (which use a slightly different class sometimes) */
    .st-emotion-cache-nahz7x { /* This might be for the button wrapper */
        border-radius: 8px; /* For recent activity buttons */
    }


    /* Sidebar styling */
    .st-emotion-cache-16txt4y { /* Sidebar background */
        background-color: #f0f2f6; /* Lighter grey for sidebar */
        border-right: 1px solid #e0e0e0;
        border-top-right-radius: 12px;
        border-bottom-right-radius: 12px;
        box_shadow: 2px 0 5px rgba(0,0,0,0.05);
    }
    .st-emotion-cache-1wqx_4d { /* Sidebar title */
        color: #2c3e50;
        font-weight: 700;
    }

    /* Header and subheader styling */
    h1, h2, h3, h4, h5, h6 {
        color: #2c3e50; /* Darker blue for headings */
        font-weight: 600;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }

    /* Buttons styling */
    .st-emotion-cache-nahz7x > button { /* Target Streamlit button itself inside the container */
        border-radius: 8px;
        border: 1px solid #3498db;
        background-color: #3498db;
        color: white;
        padding: 10px 15px;
        font-weight: 500;
        transition: background-color 0.3s, transform 0.3s;
    }
    .st-emotion-cache-nahz7x > button:hover {
        background-color: #2980b9;
        border-color: #2980b9;
        transform: translateY(-2px);
    }
    
    /* Specific styling for the clickable metric-like buttons in Overview */
    .st-emotion-cache-nahz7x .stButton > button {
        background-color: #4CAF50; /* Green for recent activity buttons */
        border-color: #4CAF50;
        font-size: 1.1em;
        font-weight: 600;
        color: white;
    }
    .st-emotion-cache-nahz7x .stButton > button:hover {
        background-color: #45a049;
        border-color: #45a049;
    }


    /* Metric boxes */
    .st-emotion-cache-l9bibm { /* Target st.metric label */
        font-size: 1.1em;
        font-weight: 600;
        color: #555555;
    }
    .st-emotion-cache-12ttj6z { /* Target st.metric value */
        font-size: 2.2em;
        font-weight: 700;
        color: #1a1a1a;
    }

    /* Info boxes */
    .st-emotion-cache-1uj25v8 { /* Target st.info block */
        border-radius: 8px;
        background-color: #e8f5e9; /* Light green for info */
        border-left: 5px solid #4CAF50;
        padding: 15px;
        margin-bottom: 15px;
    }
    .st-emotion-cache-1uj25v8 p {
        color: #333333;
    }

    /* Warning boxes */
    .st-emotion-cache-176lqf0 { /* Target st.warning block */
        border-radius: 8px;
        background-color: #fff3e0; /* Light orange for warning */
        border-left: 5px solid #ff9800;
        padding: 15px;
        margin-bottom: 15px;
    }
    .st-emotion-cache-176lqf0 p {
        color: #333333;
    }

    /* Expander styling for news articles */
    .st-emotion-cache-r42gji { /* Target expander header */
        border-radius: 8px;
        background-color: #f8f9fa; /* Light grey for expander headers */
        padding: 10px 15px;
        margin-bottom: 5px;
        border: 1px solid #e0e0e0;
    }
    .st-emotion-cache-r42gji:hover {
        background-color: #e9ecef;
    }
    .st-emotion-cache-1w0rc8c { /* Target expander content */
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        border-top: none;
        border-bottom-left-radius: 8px;
        border-bottom-right-radius: 8px;
        padding: 15px;
        margin-bottom: 15px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --- Country Code to Name Mapping (Common Countries) ---
# This dictionary can be expanded as needed.
# It prioritizes common countries and can be extended with more comprehensive lists.
COUNTRY_CODE_TO_NAME = {
    'US': 'United States', 'CA': 'Canada', 'GB': 'United Kingdom', 'AU': 'Australia',
    'DE': 'Germany', 'FR': 'France', 'IT': 'Italy', 'ES': 'Spain', 'CH': 'Switzerland',
    'JP': 'Japan', 'CN': 'China', 'IN': 'India', 'BR': 'Brazil', 'RU': 'Russia',
    'MX': 'Mexico', 'AR': 'Argentina', 'ZA': 'South Africa', 'EG': 'Egypt',
    # Add more mappings as needed based on your data's country codes/names
    # You can add more mappings here if other data sources use different codes
    'US': 'United States',  # Example for the US
    'GB': 'United Kingdom', # Example for Great Britain
    'AF': 'Afghanistan', 'AL': 'Albania', 'DZ': 'Algeria', 'AS': 'American Samoa', 'AD': 'Andorra',
    'AO': 'Angola', 'AI': 'Anguilla', 'AQ': 'Antarctica', 'AG': 'Antigua and Barbuda',
    'AR': 'Argentina', 'AM': 'Armenia', 'AW': 'Aruba', 'AU': 'Australia', 'AT': 'Austria',
    'AZ': 'Azerbaijan', 'BS': 'Bahamas', 'BH': 'Bahrain', 'BD': 'Bangladesh',
    'BB': 'Barbados', 'BY': 'Belarus', 'BE': 'Belgium', 'BZ': 'Belize', 'BJ': 'Benin',
    'BM': 'Bermuda', 'BT': 'Bhutan', 'BO': 'Bolivia (Plurinational State of)',
    'BQ': 'Bonaire, Sint Eustatius and Saba', 'BA': 'Bosnia and Herzegovina',
    'BW': 'Botswana', 'BR': 'Brazil',
    'IO': 'British Indian Ocean Territory', 'BN': 'Brunei Darussalam', 'BG': 'Bulgaria',
    'BF': 'Burkina Faso', 'BI': 'Burundi', 'CV': 'Cabo Verde', 'KH': 'Cambodia',
    'CM': 'Cameroon', 'CA': 'Canada', 'KY': 'Cayman Islands',
    'CF': 'Central African Republic', 'TD': 'Chad', 'CL': 'Chile', 'CN': 'China',
    'CX': 'Christmas Island', 'CC': 'Cocos (Keeling) Islands', 'CO': 'Colombia',
    'KM': 'Comoros', 'CD': 'Congo (Democratic Republic of the)', 'CG': 'Congo',
    'CK': 'Cook Islands', 'CR': 'Costa Rica', 'HR': 'Croatia', 'CU': 'Cuba',
    'CW': 'Curaçao', 'CY': 'Cyprus', 'CZ': 'Czechia', 'CI': 'Côte d\'Ivoire',
    'DK': 'Denmark', 'DJ': 'Djibouti', 'DM': 'Dominica', 'DO': 'Dominican Republic',
    'EC': 'Ecuador', 'EG': 'Egypt', 'SV': 'El Salvador', 'GQ': 'Equatorial Guinea',
    'ER': 'Eritrea', 'EE': 'Estonia', 'SZ': 'Eswatini', 'ET': 'Ethiopia',
    'FK': 'Falkland Islands (Malvinas)', 'FO': 'Faroe Islands', 'FJ': 'Fiji',
    'FI': 'Finland', 'FR': 'France', 'GF': 'French Guiana', 'PF': 'French Polynesia',
    'TF': 'French Southern Territories', 'GA': 'Gabon', 'GM': 'Gambia', 'GE': 'Georgia',
    'DE': 'Germany', 'GH': 'Ghana', 'GI': 'Gibraltar', 'GR': 'Greece', 'GL': 'Greenland',
    'GD': 'Grenada', 'GP': 'Guadeloupe', 'GU': 'Guam', 'GT': 'Guatemala',
    'GG': 'Guernsey', 'GN': 'Guinea', 'GW': 'Guinea-Bissau', 'GY': 'Guyana',
    'HT': 'Haiti', 'HM': 'Heard Island and McDonald Islands', 'VA': 'Holy See',
    'HN': 'Honduras', 'HK': 'Hong Kong', 'HU': 'Hungary', 'IS': 'Iceland', 'IN': 'India',
    'ID': 'Indonesia', 'IR': 'Iran (Islamic Republic of)', 'IQ': 'Iraq', 'IE': 'Ireland',
    'IM': 'Isle of Man', 'IL': 'Israel', 'IT': 'Italy', 'JM': 'Jamaica', 'JP': 'Japan',
    'JE': 'Jersey', 'JO': 'Jordan', 'KZ': 'Kazakhstan', 'KE': 'Kenya', 'KI': 'Kiribati',
    'KP': 'Korea (Democratic People\'s Republic of)', 'KR': 'Korea (Republic of)',
    'KW': 'Kuwait', 'KG': 'Kyrgyzstan', 'LA': 'Lao People\'s Democratic Republic',
    'LV': 'Latvia', 'LB': 'Lebanon', 'LS': 'Lesotho', 'LR': 'Liberia', 'LY': 'Libya',
    'LI': 'Liechtenstein', 'LT': 'Lithuania', 'LU': 'Luxembourg', 'MO': 'Macao',
    'MG': 'Madagascar', 'MW': 'Malawi', 'MY': 'Malaysia', 'MV': 'Maldives', 'ML': 'Mali',
    'MT': 'Malta', 'MH': 'Marshall Islands', 'MQ': 'Martinique', 'MR': 'Mauritania',
    'MU': 'Mauritius', 'YT': 'Mayotte', 'MX': 'Mexico',
    'FM': 'Micronesia (Federated States of)', 'MD': 'Moldova (Republic of)', 'MC': 'Monaco',
    'MN': 'Mongolia', 'ME': 'Montenegro', 'MS': 'Montserrat', 'MA': 'Morocco',
    'MZ': 'Mozambique', 'MM': 'Myanmar', 'NA': 'Namibia', 'NR': 'Nauru', 'NP': 'Nepal',
    'NL': 'Netherlands', 'NC': 'New Caledonia', 'NZ': 'New Zealand', 'NI': 'Nicaragua',
    'NE': 'Niger', 'NG': 'Nigeria', 'NU': 'Niue', 'NF': 'Norfolk Island',
    'MK': 'North Macedonia', 'MP': 'Northern Mariana Islands', 'NO': 'Norway',
    'OM': 'Oman', 'PK': 'Pakistan', 'PW': 'Palau', 'PS': 'Palestine, State of',
    'PA': 'Panama', 'PG': 'Papua New Guinea', 'PY': 'Paraguay', 'PE': 'Peru',
    'PH': 'Philippines', 'PN': 'Pitcairn', 'PL': 'Poland', 'PT': 'Portugal',
    'PR': 'Puerto Rico', 'QA': 'Qatar', 'RO': 'Romania', 'RU': 'Russian Federation',
    'RW': 'Rwanda', 'RE': 'Réunion', 'BL': 'Saint Barthélemy',
    'SH': 'Saint Helena, Ascension and Tristan da Cunha', 'KN': 'Saint Kitts and Nevis',
    'LC': 'Saint Lucia', 'MF': 'Saint Martin (French part)',
    'PM': 'Saint Pierre and Miquelon', 'VC': 'Saint Vincent and the Grenadines',
    'WS': 'Samoa', 'SM': 'San Marino', 'ST': 'Sao Tome and Principe',
    'SA': 'Saudi Arabia', 'SN': 'Senegal', 'RS': 'Serbia', 'SC': 'Seychelles',
    'SL': 'Sierra Leone', 'SG': 'Singapore', 'SX': 'Sint Maarten (Dutch part)',
    'SK': 'Slovakia', 'SI': 'Slovenia', 'SB': 'Solomon Islands', 'SO': 'Somalia',
    'ZA': 'South Africa', 'GS': 'South Georgia and the South Sandwich Islands',
    'SS': 'South Sudan', 'ES': 'Spain', 'LK': 'Sri Lanka', 'SD': 'Sudan',
    'SR': 'Suriname', 'SJ': 'Svalbard and Jan Mayen', 'SE': 'Sweden', 'CH': 'Switzerland',
    'SY': 'Syrian Arab Republic', 'TW': 'Taiwan, Province of China', 'TJ': 'Tajikistan',
    'TZ': 'Tanzania, United Republic of', 'TH': 'Thailand', 'TL': 'Timor-Leste',
    'TG': 'Togo', 'TK': 'Tokelau', 'TO': 'Tonga', 'TT': 'Trinidad and Tobago',
    'TN': 'Tunisia', 'TR': 'Turkey', 'TM': 'Turkmenistan',
    'TC': 'Turks and Caicos Islands', 'TV': 'Tuvalu', 'UG': 'Uganda', 'UA': 'Ukraine',
    'AE': 'United Arab Emirates', 'GB': 'United Kingdom of Great Britain and Northern Ireland',
    'US': 'United States of America', 'UM': 'United States Minor Outlying Islands',
    'UY': 'Uruguay', 'UZ': 'Uzbekistan', 'VU': 'Vanuatu',
    'VE': 'Venezuela (Bolivarian Republic of)', 'VN': 'Viet Nam',
    'VG': 'Virgin Islands (British)', 'VI': 'Virgin Islands (U.S.)',
    'WF': 'Wallis and Futuna', 'EH': 'Western Sahara', 'YE': 'Yemen', 'ZM': 'Zambia',
    'ZW': 'Zimbabwe'
}


# --- Data Loading Functions (from MongoDB) ---
@st.cache_data(ttl=3600) # Cache data for 1 hour to avoid constant DB queries
def load_data_from_mongo(collection_name, date_field=None):
    """
    Loads data from a specified MongoDB collection into a Pandas DataFrame.
    Converts a specified date field to datetime objects if provided.
    Applies country code to name mapping if a 'country' column exists.
    """
    try:
        # Fetch all documents from the collection
        cursor = db[collection_name].find({})
        df = pd.DataFrame(list(cursor))
        
        # Convert _id to string for easier display if needed
        if '_id' in df.columns:
            df['_id'] = df['_id'].astype(str)

        if date_field and date_field in df.columns:
            # Convert to datetime, coercing errors to NaT and ensuring UTC timezone
            df[date_field] = pd.to_datetime(df[date_field], errors='coerce', utc=True)
            # Remove rows where date_field is NaT after conversion
            df.dropna(subset=[date_field], inplace=True)
        
        # Apply country code to name mapping if 'country' column exists
        if 'country' in df.columns:
            # Ensure country column is string type and convert to uppercase for mapping
            df['country'] = df['country'].astype(str).str.upper().replace(COUNTRY_CODE_TO_NAME)
            # Capitalize each word in the country name for better display consistency, e.g., 'united states' to 'United States'
            df['country'] = df['country'].apply(lambda x: ' '.join([word.capitalize() for word in x.split()]) if isinstance(x, str) else x)

        return df
    except Exception as e:
        st.error(f"Error loading data from MongoDB collection '{collection_name}': {e}")
        return pd.DataFrame()

# Load all datasets with a spinner for better UX
with st.spinner('Connecting to MongoDB and loading data... This might take a moment.'):
    df_wildfires = load_data_from_mongo("wildfires", date_field='acq_date')
    df_landslides = load_data_from_mongo("landslides", date_field='date')
    # Assuming 'news_articles' and 'gnews_articles' are your collections for news
    df_news_articles = load_data_from_mongo("news_articles", date_field='publishedAt')
    df_gnews_articles = load_data_from_mongo("gnews_articles", date_field='publishedAt') # Assuming this is the GNews collection name


# --- Session State Initialization ---
# Initialize session state variables if they don't exist
if 'page_selection' not in st.session_state:
    st.session_state.page_selection = "Overview"
if 'wildfires_filter_dates' not in st.session_state:
    st.session_state.wildfires_filter_dates = None # Stores (start_datetime, end_datetime) for slider
if 'landslides_filter_dates' not in st.session_state:
    st.session_state.landslides_filter_dates = None
if 'news_filter_dates' not in st.session_state:
    st.session_state.news_filter_dates = None
if 'generated_summary' not in st.session_state: # New state for LLM summary
    st.session_state.generated_summary = ""


# --- Gemini API Call for Summary ---
# This function is now synchronous and uses requests.
def generate_summary_sync(articles_text):
    """
    Generates a summary of the provided articles text using the Gemini API.
    This version uses the 'requests' library for synchronous HTTP calls.
    """
    prompt = (
        f"Summarize the following news articles about global disasters. "
        f"Focus on key events, involved countries, and specific disaster types mentioned. "
        f"Keep the summary concise and informative.\n\nArticles:\n{articles_text}"
    )

    # Payload for the API request
    payload = { 
        "contents": [
            { "role": "user", "parts": [{ "text": prompt }] }
        ]
    }
    
    # Get API key from environment variable
    # This key MUST be set in your docker-compose.yml or .env for this to work correctly.
    apiKey = os.getenv("GEMINI_API_KEY", "") 
    apiUrl = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={apiKey}"

    try:
        # Use requests.post for synchronous HTTP POST request
        response = requests.post(apiUrl, headers={'Content-Type': 'application/json'}, json=payload)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        
        result = response.json()
        
        if result.get("candidates") and result["candidates"][0].get("content") and \
           result["candidates"][0]["content"].get("parts") and \
           result["candidates"][0]["content"]["parts"][0].get("text"):
            return result["candidates"][0]["content"]["parts"][0]["text"]
        else:
            st.error("Error: Unexpected response structure from Gemini API.")
            print("Gemini API Response:", result) # For debugging
            return "Failed to generate summary due to unexpected API response."
    except requests.exceptions.RequestException as e:
        st.error(f"Error calling Gemini API: {e}")
        return f"Failed to generate summary due to a network or API error: {e}"
    except Exception as e:
        st.error(f"An unexpected error occurred during summarization: {e}")
        return f"Failed to generate summary due to an unexpected error: {e}"


# --- Sidebar Navigation ---
with st.sidebar:
    st.title("Navigation")
    st.markdown("---") # Separator for better aesthetics

    # Use a common key for the radio button to ensure it works smoothly with st.session_state
    current_page_from_sidebar = st.radio(
        "Go to",
        ["Overview", "Wildfires Analysis", "Landslides Analysis", "News & Trends"],
        index=["Overview", "Wildfires Analysis", "Landslides Analysis", "News & Trends"].index(st.session_state.page_selection),
        key="sidebar_radio" # Add a key
    )

    # Update page selection if sidebar radio changes, and reset specific filters
    if current_page_from_sidebar != st.session_state.page_selection:
        st.session_state.page_selection = current_page_from_sidebar
        # Reset specific filters if user navigates via sidebar to see full data range
        st.session_state.wildfires_filter_dates = None
        st.session_state.landslides_filter_dates = None
        st.session_state.news_filter_dates = None
        st.rerun() # Rerun to apply page change immediately

    st.markdown("---") # Separator


# --- Common Dashboard Elements ---
# CHANGED: Main title
st.title("Intellicrisis : Global Disaster Analysis")
st.markdown("A comprehensive look into natural disaster data from various sources.")


# --- Page Content Based on Selection ---


if st.session_state.page_selection == "Overview":
    st.header("Dashboard Overview")
    st.write("This dashboard provides insights into global wildfires, landslides, and related news articles. Use the sidebar to navigate through different analysis sections, or click the recent activity metrics below to jump directly to filtered views.")


    st.markdown("---") # Visual separator


    # Metrics with an enhanced layout
    col1, col2, col3 = st.columns(3)


    with col1:
        st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Total Wildfire Records</h3><p class='st-emotion-cache-12ttj6z'>{len(df_wildfires) if not df_wildfires.empty else 0}</p></div>", unsafe_allow_html=True)
    with col2:
        st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Total Landslide Records</h3><p class='st-emotion-cache-12ttj6z'>{len(df_landslides) if not df_landslides.empty else 0}</p></div>", unsafe_allow_html=True)
    with col3:
        total_news_articles_count = (len(df_news_articles) if not df_news_articles.empty else 0) + \
                                    (len(df_gnews_articles) if not df_gnews_articles.empty else 0)
        st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Total News Articles</h3><p class='st-emotion-cache-12ttj6z'>{total_news_articles_count}</p></div>", unsafe_allow_html=True)


    st.subheader("Recent Activity (Last 30 days) - Click to explore!")
    st.markdown("---") # Visual separator
    
    # Define the 30-day range as timezone-aware Pandas Timestamps
    # Set end_date to end of today to include all data for current day
    end_30_days_ts = pd.Timestamp.now(tz='UTC').normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    start_30_days_ts = end_30_days_ts - pd.Timedelta(days=30)


    # Convert to Python datetime objects for storing in session state and st.slider
    start_30_days_dt = start_30_days_ts.to_pydatetime()
    end_30_days_dt = end_30_days_ts.to_pydatetime()


    recent_col1, recent_col2, recent_col3 = st.columns(3)


    with recent_col1:
        if not df_wildfires.empty:
            # Filter using timezone-aware Timestamps
            wildfires_recent_count = df_wildfires[
                (df_wildfires['acq_date'] >= start_30_days_ts) &
                (df_wildfires['acq_date'] <= end_30_days_ts)
            ].shape[0]
            
            # Make it a clickable button with updated styling
            if st.button(f"🔥 **{wildfires_recent_count}** Recent Wildfires", key="recent_wildfires_btn", use_container_width=True):
                st.session_state.page_selection = "Wildfires Analysis"
                st.session_state.wildfires_filter_dates = (start_30_days_dt, end_30_days_dt)
                st.rerun() # Rerun to apply the new page and filter
        else:
            st.info("No wildfire data.")


    with recent_col2:
        if not df_landslides.empty:
            landslides_recent_count = df_landslides[
                (df_landslides['date'] >= start_30_days_ts) &
                (df_landslides['date'] <= end_30_days_ts)
            ].shape[0]
            if st.button(f"⛰️ **{landslides_recent_count}** Recent Landslides", key="recent_landslides_btn", use_container_width=True):
                st.session_state.page_selection = "Landslides Analysis"
                st.session_state.landslides_filter_dates = (start_30_days_dt, end_30_days_dt)
                st.rerun()
        else:
            st.info("No landslide data.")


    with recent_col3:
        # Use df_gnews_articles for the count as it's typically more recent/dynamic
        # And check for 'publishedAt' as it's the date field
        if not df_gnews_articles.empty and 'publishedAt' in df_gnews_articles.columns:
            gnews_recent_count = df_gnews_articles[
                (df_gnews_articles['publishedAt'] >= start_30_days_ts) &
                (df_gnews_articles['publishedAt'] <= end_30_days_ts)
            ].shape[0]
            if st.button(f"📰 **{gnews_recent_count}** Recent News Articles", key="recent_gnews_btn", use_container_width=True):
                st.session_state.page_selection = "News & Trends"
                st.session_state.news_filter_dates = (start_30_days_dt, end_30_days_dt)
                st.rerun()
        else:
            st.info("No GNews articles or 'publishedAt' field not found.")

    st.subheader("Global Disaster Map (Overview)")
    st.markdown("Visualize the distribution of recent wildfires and landslides globally.")
    st.info("💡 **Clickable Map Markers**: Hover over points for details. You can zoom and pan the map to explore specific regions.")

    # Prepare data for combined map
    map_data = pd.DataFrame()
    if not df_wildfires.empty:
        # Ensure 'latitude', 'longitude' exist and are numeric
        df_wildfires_map = df_wildfires[['latitude', 'longitude', 'acq_date', 'brightness', 'country']].copy()
        df_wildfires_map['type'] = 'Wildfire'
        df_wildfires_map['description'] = df_wildfires_map.apply(lambda row: f"Wildfire in {row['country']} on {row['acq_date'].strftime('%Y-%m-%d')} (Brightness: {row['brightness']:.2f}K)", axis=1)
        map_data = pd.concat([map_data, df_wildfires_map.rename(columns={'acq_date': 'date'})], ignore_index=True)

    if not df_landslides.empty:
        # Ensure 'latitude', 'longitude' exist and are numeric
        df_landslides_map = df_landslides[['latitude', 'longitude', 'date', 'location', 'description', 'type']].copy()
        df_landslides_map['type'] = df_landslides_map['type'].fillna('Landslide') # Ensure 'type' exists
        df_landslides_map['description'] = df_landslides_map.apply(lambda row: f"Landslide in {row['location']} on {row['date'].strftime('%Y-%m-%d')}. Type: {row['type']}", axis=1)
        map_data = pd.concat([map_data, df_landslides_map], ignore_index=True)

    if not map_data.empty:
        # Convert lat/lon to numeric, coercing errors
        map_data['latitude'] = pd.to_numeric(map_data['latitude'], errors='coerce')
        map_data['longitude'] = pd.to_numeric(map_data['longitude'], errors='coerce')
        map_data.dropna(subset=['latitude', 'longitude'], inplace=True) # Drop rows with invalid coordinates

        # Filter for recent activity for the overview map if you want a lighter map
        # For a full overview, you might want to remove this filter or make it optional
        # Here, I'll use the 'recent_activity' filter for this overview map
        map_data_recent = map_data[
            (map_data['date'] >= start_30_days_ts) &
            (map_data['date'] <= end_30_days_ts)
        ].copy() # Filter for recent activity, use .copy() to prevent SettingWithCopyWarning

        if not map_data_recent.empty:
            fig_overview_map = px.scatter_mapbox(
                map_data_recent,
                lat="latitude",
                lon="longitude",
                color="type", # Color by disaster type
                hover_name="description", # Use the combined description
                hover_data={"date": "|%Y-%m-%d", "type": True},
                color_discrete_map={'Wildfire': 'red', 'Landslide': 'blue'}, # CHANGED: Landslide color to blue
                zoom=1, # Broader initial zoom
                height=550,
                mapbox_style="carto-positron",
                title="Recent Global Disaster Incidents (Last 30 Days)"
            )
            fig_overview_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_overview_map, use_container_width=True)
        else:
            st.info("No recent disaster data available for the overview map in the last 30 days.")
    else:
        st.warning("No wildfire or landslide data found to display on the map.")


elif st.session_state.page_selection == "Wildfires Analysis":
    st.header("🔥 Wildfires Analysis")
    st.markdown("Explore global wildfire data, including locations, trends, and key metrics. Use the date slider to focus on specific periods.")
    st.markdown("---")


    if df_wildfires.empty:
        st.warning("No wildfire data available to display. Please check the `wildfires` collection in MongoDB.")
    else:
        df_wildfires_copy = df_wildfires.copy() # Use .copy() to avoid SettingWithCopyWarning


        # Drop NaT values before finding min/max dates to avoid errors
        df_wildfires_clean_dates = df_wildfires_copy.dropna(subset=['acq_date'])


        if df_wildfires_clean_dates.empty:
            st.info("No valid dates found in wildfire data for date range selection.")
            filtered_df_wildfires = pd.DataFrame() # No data to filter
        else:
            min_date_w_ts = df_wildfires_clean_dates['acq_date'].min()
            max_date_w_ts = df_wildfires_clean_dates['acq_date'].max()


            # Handle case where min and max dates are the same (e.g., only one day of data)
            if min_date_w_ts == max_date_w_ts:
                # Extend max_date by 1 day to ensure a valid slider range
                max_date_w_ts = min_date_w_ts + pd.Timedelta(days=1)


            # Convert Timestamps to Python datetime objects for slider range
            min_date_w_dt = min_date_w_ts.to_pydatetime()
            max_date_w_dt = max_date_w_ts.to_pydatetime()


            # Set initial value for slider from session state or default to full range
            default_value_w = st.session_state.wildfires_filter_dates if st.session_state.wildfires_filter_dates else (min_date_w_dt, max_date_w_dt)


            date_range_w = st.slider(
                "Select Date Range for Wildfires",
                min_value=min_date_w_dt,
                max_value=max_date_w_dt,
                value=default_value_w,
                format="YYYY-MM-DD",
                key="wildfires_date_slider" # Add a key for the slider
            )
            
            # Update session state when slider changes (only if it's different from initial filter)
            if date_range_w != st.session_state.wildfires_filter_dates:
                st.session_state.wildfires_filter_dates = date_range_w


            # Convert slider output (timezone-aware datetime) to UTC Timestamp correctly for filtering
            start_date_filter_w = pd.to_datetime(date_range_w[0], utc=True)
            end_date_filter_w = pd.to_datetime(date_range_w[1], utc=True)


            filtered_df_wildfires = df_wildfires_copy[
                (df_wildfires_copy['acq_date'] >= start_date_filter_w) &
                (df_wildfires_copy['acq_date'] <= end_date_filter_w)
            ]
        
        st.markdown("---") # Visual separator


        # Summary Metrics for filtered wildfires
        if not filtered_df_wildfires.empty:
            col_metrics1, col_metrics2, col_metrics3 = st.columns(3)
            with col_metrics1:
                st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Total Wildfire Records (Filtered)</h3><p class='st-emotion-cache-12ttj6z'>{len(filtered_df_wildfires)}</p></div>", unsafe_allow_html=True)
            with col_metrics2:
                avg_brightness = filtered_df_wildfires['brightness'].mean()
                st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Avg. Brightness (K)</h3><p class='st-emotion-cache-12ttj6z'>{avg_brightness:.2f}</p></div>", unsafe_allow_html=True)
            with col_metrics3:
                # Fix: Handle formatting when max_frp might be None or NaN (after isnull().all())
                max_frp_val = filtered_df_wildfires['frp'].max() if 'frp' in filtered_df_wildfires.columns and not filtered_df_wildfires['frp'].isnull().all() else None
                
                # Format max_frp_val only if it's a number, otherwise display 'N/A'
                display_max_frp = f"{max_frp_val:.2f}" if isinstance(max_frp_val, (int, float)) else "N/A"
                st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Max FRP (MW)</h3><p class='st-emotion-cache-12ttj6z'>{display_max_frp}</p></div>", unsafe_allow_html=True)
        else:
            st.info("No wildfire data for the selected date range to calculate metrics.")


        st.subheader("Wildfire Locations Map")
        st.markdown("Explore wildfire hotspots on the interactive map. The size and color of the marker indicate brightness.")
        # Added explanation for brightness as a styled info box
        st.info("💡 **Understanding Brightness:** The 'Brightness' values on the map represent the fire radiative temperature measured in **Kelvin (K)**. This measurement is derived from thermal infrared data captured by satellites. Higher brightness values generally indicate hotter and more intense fires, and these correspond to brighter colors and larger markers on the map.")
        if not filtered_df_wildfires.empty:
            fig_map_w = px.scatter_mapbox(
                filtered_df_wildfires,
                lat="latitude",
                lon="longitude",
                color="brightness", # Color by brightness
                size="brightness",  # Size by brightness
                hover_name="country",
                hover_data={"acq_date": "|%Y-%m-%d", "confidence": True, "frp": True, "brightness": ":.2f"}, # Format date
                color_continuous_scale=px.colors.sequential.Inferno,
                zoom=2,
                height=500,
                mapbox_style="carto-positron", # A clean, neutral map style
                title="Interactive Wildfire Map" # Add title to map
            )
            fig_map_w.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_map_w, use_container_width=True)
        else:
            st.info("No wildfire data for the selected date range to display on the map.")


        st.subheader("Brightness vs. FRP (Fire Radiative Power)")
        st.markdown("Analyze the relationship between fire brightness and radiative power, with confidence level. FRP (Fire Radiative Power) quantifies the rate of energy released by the fire.")
        if not filtered_df_wildfires.empty and 'frp' in filtered_df_wildfires.columns and 'confidence' in filtered_df_wildfires.columns:
            fig_scatter_w = px.scatter(
                filtered_df_wildfires,
                x='frp',
                y='brightness',
                color='confidence', # Color points by confidence
                hover_name='country',
                title='Wildfire Brightness vs. FRP by Confidence',
                labels={'frp': 'Fire Radiative Power (MW)', 'brightness': 'Brightness (Kelvin)'}
            )
            st.plotly_chart(fig_scatter_w, use_container_width=True)
        else:
            st.info("Relevant data (FRP or confidence) not available for scatter plot for the selected date range.")


elif st.session_state.page_selection == "Landslides Analysis":
    st.header("⛰️ Landslides Analysis")
    st.markdown("Visualize global landslide incidents and their trends over time. Filter data using the slider to explore specific periods.")
    st.markdown("---")


    if df_landslides.empty:
        st.warning("No landslide data available to display. Please check the `landslides` collection in MongoDB.")
    else:
        df_landslides_copy = df_landslides.copy() # Use .copy()


        df_landslides_clean_dates = df_landslides_copy.dropna(subset=['date'])


        if df_landslides_clean_dates.empty:
            st.info("No valid dates found in landslide data for date range selection.")
            filtered_df_landslides = pd.DataFrame()
        else:
            min_date_l_ts = df_landslides_clean_dates['date'].min()
            max_date_l_ts = df_landslides_clean_dates['date'].max()


            # Handle case where min and max dates are the same
            if min_date_l_ts == max_date_l_ts:
                max_date_l_ts = min_date_l_ts + pd.Timedelta(days=1)


            min_date_l_dt = min_date_l_ts.to_pydatetime()
            max_date_l_dt = max_date_l_ts.to_pydatetime()


            # Set initial value for slider from session state or default to full range
            default_value_l = st.session_state.landslides_filter_dates if st.session_state.landslides_filter_dates else (min_date_l_dt, max_date_l_dt)


            date_range_l = st.slider(
                "Select Date Range for Landslides",
                min_value=min_date_l_dt,
                max_value=max_date_l_dt,
                value=default_value_l,
                format="YYYY-MM-DD",
                key="landslides_date_slider" # Add a key
            )
            
            # Update session state when slider changes
            if date_range_l != st.session_state.landslides_filter_dates:
                st.session_state.landslides_filter_dates = date_range_l


            start_date_filter_l = pd.to_datetime(date_range_l[0], utc=True)
            end_date_filter_l = pd.to_datetime(date_range_l[1], utc=True)


            filtered_df_landslides = df_landslides_copy[
                (df_landslides_copy['date'] >= start_date_filter_l) &
                (df_landslides_copy['date'] <= end_date_filter_l)
            ]
        
        st.markdown("---") # Visual separator


        # Summary Metrics for filtered landslides
        if not filtered_df_landslides.empty:
            col_metrics_l, _ = st.columns([0.3, 0.7]) # Use columns for metric layout
            with col_metrics_l:
                st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Total Landslide Records (Filtered)</h3><p class='st-emotion-cache-12ttj6z'>{len(filtered_df_landslides)}</p></div>", unsafe_allow_html=True)
        else:
            st.info("No landslide data for the selected date range to calculate metrics.")


        st.subheader("Landslide Locations Map")
        st.markdown("View reported landslide incidents on the map. Hover over markers for more details.")
        if not filtered_df_landslides.empty:
            fig_map_l = px.scatter_mapbox(
                filtered_df_landslides,
                lat="latitude",
                lon="longitude",
                color="type", # If 'type' column exists, color by it
                hover_name="location",
                hover_data={"date": "|%Y-%m-%d", "description": True, "source": True},
                zoom=2,
                height=500,
                mapbox_style="carto-positron",
                title="Interactive Landslide Map"
            )
            fig_map_l.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_map_l, use_container_width=True)
        else:
            st.info("No landslide data for the selected date range to display on the map.")


elif st.session_state.page_selection == "News & Trends":
    st.header("📰 News and Trends")
    st.markdown("Explore news articles related to natural disasters, analyze publication trends, and filter content by keywords, countries, and disaster types.")
    st.markdown("---")


    # Combine news datasets
    combined_news_df = pd.DataFrame()
    if not df_news_articles.empty:
        combined_news_df = pd.concat([combined_news_df, df_news_articles])
    if not df_gnews_articles.empty:
        combined_news_df = pd.concat([combined_news_df, df_gnews_articles])


    if combined_news_df.empty:
        st.warning("No news article data available to display. Please check `news_articles` and `gnews_articles` collections in MongoDB.")
    else:
        combined_news_df_copy = combined_news_df.copy() # Use .copy()


        df_news_clean_dates = combined_news_df_copy.dropna(subset=['publishedAt'])


        if df_news_clean_dates.empty:
            st.info("No valid dates found in news data for date range selection.")
            filtered_df_news = pd.DataFrame()
        else:
            min_date_n_ts = df_news_clean_dates['publishedAt'].min()
            max_date_n_ts = df_news_clean_dates['publishedAt'].max()


            # Handle case where min and max dates are the same
            if min_date_n_ts == max_date_n_ts:
                max_date_n_ts = min_date_n_ts + pd.Timedelta(days=1)


            min_date_n_dt = min_date_n_ts.to_pydatetime()
            max_date_n_dt = max_date_n_ts.to_pydatetime()


            # Set initial value for slider from session state or default to full range
            default_value_n = st.session_state.news_filter_dates if st.session_state.news_filter_dates else (min_date_n_dt, max_date_n_dt)


            date_range_n = st.slider(
                "Select Date Range for News Articles",
                min_value=min_date_n_dt,
                max_value=max_date_n_dt,
                value=default_value_n,
                format="YYYY-MM-DD",
                key="news_date_slider" # Add a key
            )
            
            # Update session state when slider changes
            if date_range_n != st.session_state.news_filter_dates:
                st.session_state.news_filter_dates = date_range_n


            start_date_filter_n = pd.to_datetime(date_range_n[0], utc=True)
            end_date_filter_n = pd.to_datetime(date_range_n[1], utc=True)


            filtered_df_news = combined_news_df_copy[
                (combined_news_df_copy['publishedAt'] >= start_date_filter_n) &
                (combined_news_df_copy['publishedAt'] <= end_date_filter_n)
            ]


        st.markdown("---") # Visual separator


        # Summary Metrics for filtered news
        if not filtered_df_news.empty:
            col_metrics_n, _ = st.columns([0.3, 0.7])
            with col_metrics_n:
                st.markdown(f"<div class='st-emotion-cache-1cypcdb'><h3>Total News Articles (Filtered)</h3><p class='st-emotion-cache-12ttj6z'>{len(filtered_df_news)}</p></div>", unsafe_allow_html=True)
        else:
            st.info("No news articles for the selected date range to calculate metrics.")


        st.subheader("News Articles Over Time")
        st.markdown("Observe the publication frequency of disaster-related news articles.")
        if not filtered_df_news.empty:
            news_daily = filtered_df_news.set_index('publishedAt').resample('D').size().reset_index(name='Count')
            fig_time_n = px.line(
                news_daily,
                x='publishedAt',
                y='Count',
                title='News Articles Published Daily',
                labels={'publishedAt': 'Date', 'Count': 'Number of Articles'},
                markers=True
            )
            fig_time_n.update_xaxes(dtick="M1", tickformat="%b\n%Y")
            st.plotly_chart(fig_time_n, use_container_width=True)
        else:
            st.info("No news article time series data for the selected date range.")


        # --- NEW: Sentiment Trend Over Time ---
        st.markdown("---")
        st.subheader("Sentiment Trend Over Time")
        st.markdown("Track the average sentiment of filtered news articles over time (daily average).")
        if not filtered_df_news.empty and 'semantic_score' in filtered_df_news.columns:
            # Ensure 'semantic_score' is numeric and handle potential NaNs
            sentiment_df = filtered_df_news.copy()
            sentiment_df['semantic_score'] = pd.to_numeric(sentiment_df['semantic_score'], errors='coerce')
            sentiment_df.dropna(subset=['semantic_score', 'publishedAt'], inplace=True)

            if not sentiment_df.empty:
                # Resample daily and calculate mean sentiment score
                sentiment_daily = sentiment_df.set_index('publishedAt').resample('D')['semantic_score'].mean().reset_index(name='Average Sentiment Score')
                
                fig_sentiment_trend = px.line(
                    sentiment_daily,
                    x='publishedAt',
                    y='Average Sentiment Score',
                    title='Average Sentiment Score of News Articles Daily',
                    labels={'publishedAt': 'Date', 'Average Sentiment Score': 'Average Sentiment'},
                    markers=True
                )
                # Add horizontal lines for typical sentiment ranges (e.g., neutral at 0)
                fig_sentiment_trend.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="Neutral Sentiment", annotation_position="bottom right")
                fig_sentiment_trend.update_xaxes(dtick="M1", tickformat="%b\n%Y")
                
                st.plotly_chart(fig_sentiment_trend, use_container_width=True)
            else:
                st.info("No valid sentiment data available for the selected date range.")
        else:
            st.info("Sentiment data ('semantic_score') not available for trend analysis.")
        # --- END NEW: Sentiment Trend Over Time ---


        st.markdown("---") # Visual separator
        st.subheader("Filter and Display News Articles")
        st.markdown("Use the filters below to find specific news articles.")


        # News Article Filtering with new layout
        col_filter1, col_filter2, col_filter3 = st.columns(3) # Three columns for filters

        with col_filter1:
            keyword_search = st.text_input("Search by Keyword (in title/description):", "")

        with col_filter2:
            # Get unique disaster types from all combined news articles (using 'disaster_type')
            all_disaster_types = []
            if not combined_news_df.empty and 'disaster_type' in combined_news_df.columns:
                # Handle cases where 'disaster_type' might be a list or a single string
                for types_list in combined_news_df['disaster_type'].dropna():
                    if isinstance(types_list, list):
                        all_disaster_types.extend(types_list)
                    else:
                        all_disaster_types.append(types_list)
            
            unique_disaster_types = sorted(list(set(all_disaster_types)))
            
            selected_disaster_type = st.selectbox(
                "Filter by Disaster Type:",
                ["All"] + unique_disaster_types,
                index=0 # Default to "All"
            )
        
        with col_filter3:
            # Get unique countries from all combined news articles
            # The country column is now standardized to full names during load_data_from_mongo
            all_countries = combined_news_df['country'].dropna().unique().tolist()
            unique_countries = sorted(list(set(all_countries)))
            
            selected_countries = st.multiselect(
                "Filter by Country:",
                unique_countries,
                default=[] # No default selection
            )


        # Apply all filters sequentially
        if keyword_search:
            filtered_df_news = filtered_df_news[
                filtered_df_news['title'].fillna('').str.contains(keyword_search, case=False, na=False) |
                filtered_df_news['description'].fillna('').str.contains(keyword_search, case=False, na=False)
            ]

        if selected_disaster_type != "All":
            filtered_df_news = filtered_df_news[
                filtered_df_news['disaster_type'].apply(
                    lambda x: selected_disaster_type in x if isinstance(x, list) else x == selected_disaster_type
                )
            ]
        
        if selected_countries: # Only apply if countries are selected
            filtered_df_news = filtered_df_news[filtered_df_news['country'].isin(selected_countries)]


        st.markdown(f"**Displaying {len(filtered_df_news)} filtered articles.**")

        # --- LLM Summary Section ---
        st.markdown("---")
        st.subheader("LLM-Generated Summary of Filtered Articles")
        st.write("Click the button below to generate a concise summary of the articles currently displayed based on your filters.")

        # Create a combined text for the LLM
        articles_for_llm_summary = ""
        if not filtered_df_news.empty:
            # Combine title and description for each article, handling potential NaNs
            articles_for_llm_summary = "\n\n".join(
                (
                    (str(row['title']) if pd.notna(row['title']) else '') + ". " + 
                    (str(row['description']) if pd.notna(row['description']) else '')
                ).strip()
                for index, row in filtered_df_news.iterrows()
                if pd.notna(row['title']) or pd.notna(row['description']) # Only include if title or desc exists
            )
        
        # Display a warning if there are too many articles for summarization
        if len(articles_for_llm_summary) > 5000: # Arbitrary token limit estimate
            st.warning("The number of articles for summarization is very large. This might lead to truncation or a less coherent summary from the LLM. Consider narrowing your filters.")

        if st.button("Generate Summary"):
            if not articles_for_llm_summary:
                st.info("No articles to summarize based on current filters.")
                st.session_state.generated_summary = "No articles available for summarization."
            else:
                with st.spinner("Generating summary... This might take a moment."):
                    st.session_state.generated_summary = generate_summary_sync(articles_for_llm_summary)
        
        if st.session_state.generated_summary:
            st.markdown("---") # Add a separator before displaying the summary
            st.text_area(
                "Summary:",
                value=st.session_state.generated_summary,
                height=300,
                key="summary_text_area",
                help="An AI-generated summary of the filtered news articles."
            )
            if st.button("Clear Summary", key="clear_summary_button"):
                st.session_state.generated_summary = ""
                st.rerun()

        st.markdown("---") # Separator before article list


        if not filtered_df_news.empty:
            # Sort by publishedAt descending
            filtered_df_news_sorted = filtered_df_news.sort_values(by='publishedAt', ascending=False)
            
            # Display articles in expanders
            for index, row in filtered_df_news_sorted.iterrows():
                # Format date for display
                published_date_str = row['publishedAt'].strftime('%Y-%m-%d %H:%M UTC') if pd.notna(row['publishedAt']) else "N/A"
                
                # Use source and author for header
                source_display = row['source'] if pd.notna(row['source']) else "Unknown Source"
                # Use .get() to safely access 'author'
                author_val = row.get('author')
                author_display = f" by {author_val}" if pd.notna(author_val) and author_val != 'None' else ""
                
                with st.expander(f"**{row['title']}** - {source_display}{author_display} ({published_date_str})"):
                    if pd.notna(row['description']):
                        st.write(row['description'])
                    if pd.notna(row['url']):
                        st.markdown(f"[Read full article]({row['url']})", unsafe_allow_html=True)
                    
                    # Display disaster types if available
                    if 'disaster_type' in row and pd.notna(row['disaster_type']):
                        if isinstance(row['disaster_type'], list):
                            st.markdown(f"**Disaster Types:** {', '.join(row['disaster_type'])}")
                        else:
                            st.markdown(f"**Disaster Type:** {row['disaster_type']}")
                    
                    # Display country if available
                    if 'country' in row and pd.notna(row['country']):
                        st.markdown(f"**Country:** {row['country']}")

                    # Display image if available (ensure 'urlToImage' exists and is a valid URL)
                    if 'urlToImage' in row and pd.notna(row['urlToImage']) and row['urlToImage'].startswith('http'):
                        try:
                            st.image(row['urlToImage'], caption=row['title'], use_column_width=True)
                        except Exception as e:
                            st.warning(f"Could not load image for article '{row['title']}': {e}")
                            st.info("Image URL might be broken or inaccessible.")


        else:
            st.info("No articles match the current filter criteria.")