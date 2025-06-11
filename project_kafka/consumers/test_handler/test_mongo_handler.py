# test_mongo_handler.py
import os
import sys

# Adjust PYTHONPATH so imports work correctly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from storage.mongo_handler import store_to_mongo

def test_store():
    test_record = {
        "title": "Test Article from test_mongo_handler",
        "content": "This is a test content for MongoDB insertion.",
        "source": "test_source",
        "timestamp": "2025-06-09T16:00:00"
    }
    topic = "test_topic"

    success = store_to_mongo(test_record, topic)
    print("MongoDB insertion success:", success)

if __name__ == "__main__":
    test_store()