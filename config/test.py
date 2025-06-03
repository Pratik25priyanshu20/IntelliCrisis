'''
import requests
from datetime import datetime, timedelta
import json

# Your API key
API_KEY = "1d824cccd8474ecb88d8e7bdf5897bc6"

def test_api_basic():
    """Test basic API functionality"""
    print("🔧 Testing basic API connection...")
    
    url = f"https://newsapi.org/v2/everything?q=Germany&from=2024-05-01&to=2024-05-02&apiKey={API_KEY}"
    
    try:
        response = requests.get(url, timeout=30)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ API Working! Total Results: {data.get('totalResults', 0)}")
            print(f"Articles Returned: {len(data.get('articles', []))}")
            
            # Show first article if available
            if data.get('articles'):
                first_article = data['articles'][0]
                print(f"\nFirst Article Example:")
                print(f"Title: {first_article.get('title', 'N/A')}")
                print(f"Source: {first_article.get('source', {}).get('name', 'N/A')}")
                print(f"Published: {first_article.get('publishedAt', 'N/A')}")
            
            return True
            
        elif response.status_code == 401:
            print("❌ API Key Invalid or Unauthorized")
            print("Response:", response.text)
            return False
            
        elif response.status_code == 429:
            print("❌ Rate limit exceeded")
            print("Response:", response.text)
            return False
            
        else:
            print(f"❌ API Error: {response.status_code}")
            print("Response:", response.text)
            return False
            
    except Exception as e:
        print(f"❌ Network Error: {e}")
        return False

def test_disaster_queries():
    """Test disaster-specific queries"""
    print("\n🔥 Testing disaster-specific queries...")
    
    # Test queries
    test_queries = [
        "wildfire Germany",
        "landslide Switzerland", 
        "Waldbrand Deutschland",
        "Erdrutsch Schweiz"
    ]
    
    for query in test_queries:
        print(f"\nTesting query: '{query}'")
        
        # Use recent dates
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        url = (f"https://newsapi.org/v2/everything?"
               f"q={query}&"
               f"from={start_date.strftime('%Y-%m-%d')}&"
               f"to={end_date.strftime('%Y-%m-%d')}&"
               f"language=en,de&"
               f"sortBy=publishedAt&"
               f"apiKey={API_KEY}")
        
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                total = data.get('totalResults', 0)
                articles = len(data.get('articles', []))
                print(f"  ✅ Results: {total} total, {articles} returned")
                
                # Show one relevant title if found
                for article in data.get('articles', [])[:1]:
                    print(f"  📰 Example: {article.get('title', 'No title')[:80]}...")
                    
            else:
                print(f"  ❌ Error: {response.status_code}")
                
        except Exception as e:
            print(f"  ❌ Error: {e}")
        
        # Small delay between requests
        import time
        time.sleep(1)

def check_api_limits():
    """Check API plan limits"""
    print("\n📊 Checking API plan...")
    
    # Try to get sources (this endpoint shows your plan info in headers)
    url = f"https://newsapi.org/v2/sources?apiKey={API_KEY}"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print("✅ API Key is valid")
            
            # Check headers for rate limit info
            headers = response.headers
            if 'X-RateLimit-Remaining' in headers:
                print(f"Requests remaining: {headers['X-RateLimit-Remaining']}")
            if 'X-RateLimit-Limit' in headers:
                print(f"Total requests limit: {headers['X-RateLimit-Limit']}")
                
        else:
            print(f"❌ Sources endpoint error: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error checking limits: {e}")

if __name__ == "__main__":
    print("🚀 NewsAPI Key Testing Started")
    print("=" * 50)
    
    # Test basic functionality
    if test_api_basic():
        # If basic test works, try disaster queries
        test_disaster_queries()
        check_api_limits()
    else:
        print("\n❌ Basic API test failed. Please check your API key.")
        print("\nTroubleshooting steps:")
        print("1. Verify your API key at https://newsapi.org/account")
        print("2. Check if you've exceeded your monthly quota")
        print("3. Ensure your account is active")
    
    print("\n" + "=" * 50)
    print("🏁 Testing Complete")
    
    
    
import requests

API_KEY = "5a171490a2f8a2007d6a098e92cc5758"
url = f"http://api.mediastack.com/v1/news?access_key={API_KEY}&keywords=Germany&limit=1"

print("🔧 Testing Mediastack API Key...")
response = requests.get(url)

print(f"Status Code: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    print("✅ Key is valid. Sample article:")
    print("🔹 Title:", data["data"][0]["title"])
    print("🔹 Source:", data["data"][0]["source"])
else:
    print("❌ Error:", response.text)
    
    '''
    
    
import requests
from datetime import datetime, timedelta

# ✅ Your NewsAPI key
NEWSAPI_KEY = "1d824cccd8474ecb88d8e7bdf5897bc6"

# ✅ Expanded query keywords
query = "Germany "


# 📅 Date range (last 30 days)
from_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
to_date = datetime.utcnow().strftime("%Y-%m-%d")

# 🌍 Construct the query URL
url = (
    f"https://newsapi.org/v2/everything?"
    f"q={query}&"
    f"from={from_date}&"
    f"to={to_date}&"
    f"sortBy=relevancy&"
    f"language=de,en&"
    f"pageSize=50&"
    f"apiKey={NEWSAPI_KEY}"
)

# 🚀 Perform the request
print("🔍 Testing enhanced NewsAPI query...\n")
response = requests.get(url)
print(f"Status Code: {response.status_code}")

# ✅ Parse and display result
if response.status_code == 200:
    articles = response.json().get("articles", [])
    print(f"✅ Success! Articles fetched: {len(articles)}")
    if articles:
        print("📰 Sample article:")
        print("Title:", articles[0]['title'])
        print("Source:", articles[0]['source']['name'])
        print("Published:", articles[0]['publishedAt'])
        print("URL:", articles[0]['url'])
else:
    print("❌ Error:", response.json())