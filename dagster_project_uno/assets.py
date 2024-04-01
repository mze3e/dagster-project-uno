"""
assets.py for dagster_project_uno.
"""

import json
import os
import base64
from io import BytesIO
import requests
from dagster_dbt import DbtCliClientResource
from dagster_dbt import load_assets_from_dbt_project

import matplotlib.pyplot as plt

from dagster import AssetExecutionContext, MetadataValue, asset, MaterializeResult
import pandas as pd  # Add new imports to the top of `assets.py`

from .resources import DataGeneratorResource

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")

resources = {
    "dbt": DbtCliClientResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROJECT_DIR,
    ),
}

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR, key_prefix=["dbt_assets"]
)

@asset # add the asset decorator to tell Dagster this is an asset
def topstory_ids() -> MaterializeResult:
    """
    Fetches the IDs of the top stories from Hacker News API and saves them to a JSON file.
    """
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_new_story_ids = requests.get(newstories_url, timeout=5).json()[:100]

    os.makedirs("data", exist_ok=True)
    with open("data/topstory_ids.json", "w", encoding="utf8") as f:
        json.dump(top_new_story_ids, f)

    return MaterializeResult(
            metadata={
                "num_records": len(top_new_story_ids),  # Metadata can be any key-value pair
                "preview": MetadataValue.md(f"Top story IDs: {str(top_new_story_ids[:5])}"),
                # The `MetadataValue` class has useful static methods to build Metadata
            }
    )

@asset(deps=[topstory_ids])  # this asset is dependent on topstory_ids
def topstories(context: AssetExecutionContext) -> MaterializeResult:
    """
    Fetches the top stories from Hacker News API and saves them as a CSV file.

    This function reads the top story IDs from a JSON file, 
    makes API requests to retrieve the details of each story,
    and appends them to a list. After fetching a certain number of stories, 
    it prints the count. Finally, it converts the list of stories into a 
    pandas DataFrame and saves it as a CSV file.

    Args:
        None

    Returns:
        None
    """
    with open("data/topstory_ids.json", "r", encoding="utf8") as f:
        ids_of_top_stories = json.load(f)

    results = []
    for item_id in ids_of_top_stories:
        item = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json",
            timeout=5
        ).json()
        results.append(item)

        if len(results) % 20 == 0:
            context.log.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)
    df.to_csv("data/topstories.csv")

    return MaterializeResult(
            metadata={
                "num_records": len(df),  # Metadata can be any key-value pair
                "preview": MetadataValue.md(df.head().to_markdown()),
                # The `MetadataValue` class has useful static methods to build Metadata
            }
    )

@asset(deps=[topstories])
def most_frequent_words() -> MaterializeResult:
    """
    Calculate the frequency of words in the titles of top stories and store
    the top 25 most frequent words in a JSON file. This function reads a CSV 
    file containing top stories and loops through the titles to count the 
    frequency of each word. It removes common stopwords and punctuation marks 
    from the words before counting. The function then selects the top 25 most 
    frequent words and stores them in a JSON file.

    Args:
        None

    Returns:
        None
    """
    stopwords = ["a", "the", "an", "of", "to", "in", "for", "and", "with", "on", "is"]

    top_stories = pd.read_csv("data/topstories.csv")

    # loop through the titles and count the frequency of each word
    word_counts = {}
    for raw_title in top_stories["title"]:
        title = raw_title.lower()
        for word in title.split():
            cleaned_word = word.strip(".,-!?:;()[]'\"-")
            if cleaned_word not in stopwords and len(cleaned_word) > 0:
                word_counts[cleaned_word] = word_counts.get(cleaned_word, 0) + 1

    # Get the top 25 most frequent words
    top_words = {
        pair[0]: pair[1]
        for pair in sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:25]
    }

    # Make a bar chart of the top 25 words
    plt.figure(figsize=(10, 6))
    plt.bar(list(top_words.keys()), list(top_words.values()))
    plt.xticks(rotation=45, ha="right")
    plt.title("Top 25 Words in Hacker News Titles")
    plt.tight_layout()

    # Convert the image to a saveable format
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())

    # Convert the image to Markdown to preview it within Dagster
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    with open("data/most_frequent_words.json", "w", encoding="utf8") as f:
        json.dump(top_words, f)

    # Attach the Markdown content as metadata to the asset
    return MaterializeResult(metadata={"plot": MetadataValue.md(md_content)})

@asset
def signups(hackernews_api: DataGeneratorResource) -> MaterializeResult:
    """
    Retrieves signups data from the HackerNews API and saves it to a CSV file.

    Args:
        hackernews_api (DataGeneratorResource): The HackerNews API data generator resource.

    Returns:
        MaterializeResult: The result of materializing the signups data, 
        including metadata such as record count, preview, earliest signup, 
        and latest signup.
    """
    signups_df = pd.DataFrame(hackernews_api.get_signups())

    signups_df.to_csv("data/signups.csv")

    return MaterializeResult(
        metadata={
            "Record Count": len(signups_df),
            "Preview": MetadataValue.md(signups_df.head().to_markdown()),
            "Earliest Signup": signups_df["registered_at"].min(),
            "Latest Signup": signups_df["registered_at"].max(),
        }
    )
