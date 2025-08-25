#!/usr/bin/env python3
"""
Test cases for fuzzy VIN matcher
Run with: python test_fuzzy_matcher.py
"""

import pytest
from fuzzy_vin_matcher import (
    extract_names_from_title,
    build_assets_index,
    top_matches_for_name,
    shortlist_for_group_title,
    normalize_name
)

def test_extract_names_from_title():
    """Test name extraction from various group title formats"""
    
    test_cases = [
        {
            "title": "198 - S* - Abdul Rashid Bigzad - (C) - Truck_041. Phone: (770) 912-5431",
            "expected": ["Abdul Rashid Bigzad"]
        },
        {
            "title": "090 - N* - Sherzod Pirmetov / Jamoliddin Sodikov - (O) - Truck_7062. Phone: ...",
            "expected": ["Sherzod Pirmetov", "Jamoliddin Sodikov"]
        },
        {
            "title": "111 - B* - Rafael Suarez / Gretzin Sanchez - (C) - Truck_3834. Phone: ...",
            "expected": ["Rafael Suarez", "Gretzin Sanchez"]
        },
        {
            "title": "Carlos Martinez - Driver",
            "expected": ["Carlos Martinez"]
        },
        {
            "title": "123 - Numbers Only - 456",
            "expected": []
        },
        {
            "title": "Just Random Text",
            "expected": []
        },
        {
            "title": "",
            "expected": []
        }
    ]
    
    for case in test_cases:
        result = extract_names_from_title(case["title"])
        print(f"Title: '{case['title']}'")
        print(f"Expected: {case['expected']}")
        print(f"Got: {result}")
        assert result == case["expected"], f"Failed for: {case['title']}"
        print("✅ Passed\n")

def test_build_assets_index():
    """Test building assets index from sheet data"""
    
    # Mock sheet data (with header)
    mock_data = [
        ["Timestamp", "Gateway", "Serial", "Driver Name", "VIN", "Location"],  # Header
        ["", "GW001", "S001", "Rafael Suarez", "1XKWD49X5NR476547", ""],
        ["", "GW002", "S002", "Gretzin Sanchez", "3AKJHHDR3KSHU6562", ""],
        ["", "", "", "", "", ""],  # Empty row
        ["", "GW003", "S003", "John", "INVALID_VIN", ""],  # Invalid VIN
        ["", "GW004", "S004", "Carlos Martinez Rodriguez", "3AKJHHFG3SSVR2041", ""]
    ]
    
    assets = build_assets_index(mock_data, driver_col=3, vin_col=4)
    
    expected = [
        ("Rafael Suarez", "1XKWD49X5NR476547"),
        ("Gretzin Sanchez", "3AKJHHDR3KSHU6562"),
        ("Carlos Martinez Rodriguez", "3AKJHHFG3SSVR2041")
    ]
    
    print(f"Built assets index: {assets}")
    assert assets == expected
    print("✅ Build assets index test passed\n")

def test_top_matches_for_name():
    """Test fuzzy matching for names"""
    
    assets = [
        ("Rafael Suarez", "1XKWD49X5NR476547"),
        ("Gretzin Sanchez", "3AKJHHDR3KSHU6562"),
        ("Carlos Martinez", "3AKJHHFG3SSVR2041"),
        ("Abdul Rashid Bigzad", "4V4NC9EH6GN946170")
    ]
    
    test_cases = [
        {
            "query": "Rafael",
            "expected_top": "Rafael Suarez",
            "min_score": 80
        },
        {
            "query": "Gretzin",
            "expected_top": "Gretzin Sanchez", 
            "min_score": 80
        },
        {
            "query": "Martinez",
            "expected_top": "Carlos Martinez",
            "min_score": 70
        },
        {
            "query": "Abdul Rashid",
            "expected_top": "Abdul Rashid Bigzad",
            "min_score": 85
        }
    ]
    
    for case in test_cases:
        matches = top_matches_for_name(case["query"], assets, k=3)
        print(f"Query: '{case['query']}'")
        print(f"Matches: {[(name, vin[-4:], score) for name, vin, score in matches]}")
        
        assert len(matches) > 0, f"No matches found for: {case['query']}"
        
        top_match = matches[0]
        assert top_match[0] == case["expected_top"], f"Expected {case['expected_top']}, got {top_match[0]}"
        assert top_match[2] >= case["min_score"], f"Score too low: {top_match[2]} < {case['min_score']}"
        
        print("✅ Passed\n")

def test_shortlist_for_group_title():
    """Test complete shortlist generation"""
    
    assets = [
        ("Rafael Suarez", "1XKWD49X5NR476547"),
        ("Gretzin Sanchez", "3AKJHHDR3KSHU6562"),
        ("Carlos Martinez", "3AKJHHFG3SSVR2041"),
        ("Abdul Rashid Bigzad", "4V4NC9EH6GN946170"),
        ("Sherzod Pirmetov", "3AKJGLDR3HSJJ9954")
    ]
    
    test_cases = [
        {
            "title": "111 - B* - Rafael Suarez / Gretzin Sanchez - (C) - Truck_3834. Phone: ...",
            "expected_drivers": ["Rafael Suarez", "Gretzin Sanchez"],
            "min_matches": 2
        },
        {
            "title": "198 - S* - Abdul Rashid Bigzad - (C) - Truck_041. Phone: (770) 912-5431",
            "expected_drivers": ["Abdul Rashid Bigzad"],
            "min_matches": 1
        }
    ]
    
    for case in test_cases:
        shortlist = shortlist_for_group_title(case["title"], assets, k_each=3)
        print(f"Title: '{case['title']}'")
        print(f"Shortlist: {[(name, vin[-4:], score) for name, vin, score in shortlist]}")
        
        assert len(shortlist) >= case["min_matches"], f"Expected at least {case['min_matches']} matches"
        
        # Check if expected drivers are in the results
        found_drivers = [match[0] for match in shortlist]
        for expected_driver in case["expected_drivers"]:
            assert expected_driver in found_drivers, f"Expected driver {expected_driver} not found"
        
        print("✅ Passed\n")

def test_normalize_name():
    """Test name normalization"""
    
    test_cases = [
        ("Rafael Suarez", "rafael suarez"),
        ("O'Connor", "o'connor"),
        ("Jean-Pierre", "jean-pierre"),
        ("  Multiple   Spaces  ", "multiple spaces"),
        ("Punctuation!@#$%", "punctuation"),
        ("", "")
    ]
    
    for input_name, expected in test_cases:
        result = normalize_name(input_name)
        print(f"'{input_name}' -> '{result}' (expected: '{expected}')")
        assert result == expected
        print("✅ Passed\n")

if __name__ == "__main__":
    print("🧪 Running Fuzzy VIN Matcher Tests\n")
    
    try:
        test_normalize_name()
        test_extract_names_from_title()
        test_build_assets_index()
        test_top_matches_for_name()
        test_shortlist_for_group_title()
        
        print("🎉 All tests passed!")
        
    except AssertionError as e:
        print(f"❌ Test failed: {e}")
    except Exception as e:
        print(f"💥 Unexpected error: {e}")

# Quick demo
def demo():
    """Quick demonstration of the system"""
    print("\n🔍 Quick Demo:")
    
    # Sample assets data
    assets = [
        ("Rafael Suarez", "1XKWD49X5NR476547"),
        ("Gretzin Sanchez", "3AKJHHDR3KSHU6562"),
        ("Carlos Martinez", "3AKJHHFG3SSVR2041"),
        ("Abdul Rashid Bigzad", "4V4NC9EH6GN946170"),
        ("Sherzod Pirmetov", "3AKJGLDR3HSJJ9954")
    ]
    
    # Sample group titles
    titles = [
        "111 - B* - Rafael Suarez / Gretzin Sanchez - (C) - Truck_3834. Phone: ...",
        "198 - S* - Abdul Rashid Bigzad - (C) - Truck_041. Phone: (770) 912-5431",
        "090 - N* - Sherzod Pirmetov / Random Name - (O) - Truck_7062. Phone: ..."
    ]
    
    for title in titles:
        print(f"\nGroup: '{title[:60]}...'")
        shortlist = shortlist_for_group_title(title, assets, k_each=2)
        
        if shortlist:
            print("Suggestions:")
            for i, (driver, vin, score) in enumerate(shortlist[:3]):
                conf = "✅" if score >= 70 else "⚠️"
                print(f"  {conf} {score}% • {driver} • {vin[:8]}...")
        else:
            print("  No matches found")

if __name__ == "__main__" and len(__import__('sys').argv) > 1 and __import__('sys').argv[1] == "demo":
    demo()