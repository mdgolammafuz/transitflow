import pytest
from unittest.mock import patch
from rag.driver import find_revenue_driver

# 1. Define sample data that mimics what's inside chunks.pkl
MOCK_CORPUS = [
    {
        "text": "Microsoft's revenue growth was driven by Intelligent Cloud and Azure services.",
        "meta": {"company": "MSFT", "year": 2023}
    },
    {
        "text": "Apple's revenue decline was due to lower iPhone sales.",
        "meta": {"company": "AAPL", "year": 2023}
    },
    {
        "text": "Irrelevant text about general market conditions.",
        "meta": {"company": "MSFT", "year": 2023}
    }
]

@patch("rag.driver._load_corpus") # 2. Patch the function that reads the file
def test_msft_driver_found(mock_load):
    # 3. Tell the mock to return our sample data
    mock_load.return_value = MOCK_CORPUS
    
    # 4. Run the function (it will use the mock data instead of opening the file)
    res = find_revenue_driver(company="MSFT")
    
    # 5. Assertions
    assert res is not None
    assert "Intelligent Cloud" in res["text"]
    # Verify we actually filtered correctly
    assert res["meta"]["company"] == "MSFT"

def test_driver_not_found():
    # Test graceful failure
    with patch("rag.driver._load_corpus", return_value=MOCK_CORPUS):
        res = find_revenue_driver(company="NONEXISTENT")
        assert res is None or res == {}