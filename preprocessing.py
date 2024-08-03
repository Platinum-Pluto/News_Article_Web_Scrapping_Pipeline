"""import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Load the dataset
file_path = 'Extracted-News-Articles_1.xlsx'  # Replace with your dataset file path
df = pd.read_excel(file_path)

# Function to clean the metadata string
def clean_metadata_string(metadata):
    metadata = metadata.replace("'", "\"")  # Replace single quotes with double quotes
    metadata = metadata.replace("\n", " ")  # Remove newlines
    metadata = metadata.replace("\r", " ")  # Remove carriage returns
    return metadata

# Function to extract the required information from metadata
def extract_metadata_info(metadata):
    metadata = clean_metadata_string(metadata)
    try:
        metadata_dict = json.loads(metadata)  # Convert string to dictionary
        news_id = metadata_dict.get('og:url', '').split('/')[-1]  # Assuming unique ID is part of the URL
        keywords = metadata_dict.get('keywords', '')
        date_time = metadata_dict.get('article:published_time', '')
    except json.JSONDecodeError as e:
        print(f"JSON decode error for metadata: {metadata}\nError: {e}")
        news_id = keywords = date_time = None
    return news_id, keywords, date_time

# Function to process each row
def process_row(index, row):
    metadata = row['Metadata']
    news_id, keywords, date_time = extract_metadata_info(metadata)
    return index, news_id, keywords, date_time

# Initialize new columns
df['News ID'] = ''
df['Keywords'] = ''
df['Date and Time'] = ''

# Use ThreadPoolExecutor for parallel processing
with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(tqdm(executor.map(lambda idx_row: process_row(*idx_row), df.iterrows()), total=len(df)))

# Update the DataFrame with the results
for index, news_id, keywords, date_time in results:
    if news_id is not None and keywords is not None and date_time is not None:
        df.at[index, 'News ID'] = news_id
        df.at[index, 'Keywords'] = keywords
        df.at[index, 'Date and Time'] = date_time

# Save the modified dataset to a new Excel file
output_file_path = 'preprocessed_dataset.xlsx'
df.to_excel(output_file_path, index=False)
"""

"""

import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Load the dataset
file_path = 'Extracted-News-Articles_1.xlsx'  # Replace with your dataset file path
df = pd.read_excel(file_path)

# Function to extract the required information from metadata using regular expressions
def extract_metadata_info(metadata):
    news_id = re.search(r"'og:url': '.*\/(.*?)'", metadata)
    keywords = re.search(r"'keywords': '(.*?)'", metadata)
    date_time = re.search(r"'article:published_time': '(.*?)'", metadata)
    author = re.search(r"'author': '(.*?)'", metadata)
    
    news_id = news_id.group(1) if news_id else ''
    keywords = keywords.group(1) if keywords else ''
    date_time = date_time.group(1) if date_time else ''
    author = author.group(1) if author else ''
    
    return news_id, keywords, date_time, author

# Function to process each row
def process_row(index, row):
    metadata = row['Metadata']
    news_id, keywords, date_time, author = extract_metadata_info(metadata)
    return index, news_id, keywords, date_time, author

# Initialize new columns
df['News ID'] = ''
df['Keywords'] = ''
df['Date and Time'] = ''
df['Author'] = ''

# Use ThreadPoolExecutor for parallel processing
with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(tqdm(executor.map(lambda idx_row: process_row(*idx_row), df.iterrows()), total=len(df)))

# Update the DataFrame with the results
for index, news_id, keywords, date_time, author in results:
    df.at[index, 'News ID'] = news_id
    df.at[index, 'Keywords'] = keywords
    df.at[index, 'Date and Time'] = date_time
    df.at[index, 'Author'] = author

# Save the modified dataset to a new Excel file
output_file_path = 'modified_dataset.xlsx'
df.to_excel(output_file_path, index=False)
"""

"""
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Load the dataset
file_path = 'Extracted-News-Articles_1.xlsx'  # Replace with your dataset file path
df = pd.read_excel(file_path)

# Function to extract the required information from metadata using regular expressions
def extract_metadata_info(metadata):
    news_id = re.search(r"'og:url': '.*\/(.*?)'", metadata)
    keywords = re.search(r"'keywords': '(.*?)'", metadata)
    date_time = re.search(r"'article:published_time': '(.*?)'", metadata)
    author = re.search(r"'author': '(.*?)'", metadata)
    description = re.search(r"'description': '(.*?)'", metadata)
    
    news_id = news_id.group(1) if news_id else ''
    keywords = keywords.group(1) if keywords else ''
    date_time = date_time.group(1) if date_time else ''
    author = author.group(1) if author else ''
    description = description.group(1) if description else ''
    
    return news_id, keywords, date_time, author, description

# Function to process each row
def process_row(index, row):
    metadata = row['Metadata']
    news_id, keywords, date_time, author, description = extract_metadata_info(metadata)
    return index, news_id, keywords, date_time, author, description

# Initialize new columns
df['News ID'] = ''
df['Keywords'] = ''
df['Date and Time'] = ''
df['Author'] = ''
df['Description'] = ''

# Use ThreadPoolExecutor for parallel processing
with ThreadPoolExecutor(max_workers=5) as executor:
    results = list(tqdm(executor.map(lambda idx_row: process_row(*idx_row), df.iterrows()), total=len(df)))

# Update the DataFrame with the results
for index, news_id, keywords, date_time, author, description in results:
    df.at[index, 'News ID'] = news_id
    df.at[index, 'Keywords'] = keywords
    df.at[index, 'Date and Time'] = date_time
    df.at[index, 'Author'] = author
    df.at[index, 'Description'] = description

# Save the modified dataset to a new Excel file
output_file_path = 'preprocessed_dataset.xlsx'
df.to_excel(output_file_path, index=False)
"""

import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

file_path = 'Extracted-News-Articles_1.xlsx'  
df = pd.read_excel(file_path)

def extract_metadata_info(metadata):
    news_id = re.search(r"'og:url': '.*\/(.*?)'", metadata)
    keywords = re.search(r"'[^']*keywords[^']*': '(.*?)'", metadata)  
    date_time = re.search(r"'article:published_time': '(.*?)'", metadata)
    author = re.search(r"'author': '(.*?)'", metadata)
    description = re.search(r"'description': '(.*?)'", metadata)
    news_id = news_id.group(1) if news_id else ''
    keywords = keywords.group(1) if keywords else ''
    date_time = date_time.group(1) if date_time else ''
    author = author.group(1) if author else ''
    description = description.group(1) if description else ''
    
    return news_id, keywords, date_time, author, description


def process_row(index, row):
    metadata = row['Metadata']
    news_id, keywords, date_time, author, description = extract_metadata_info(metadata)
    return index, news_id, keywords, date_time, author, description


df['News ID'] = ''
df['Keywords'] = ''
df['Date and Time'] = ''
df['Author'] = ''
df['Description'] = ''


with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(tqdm(executor.map(lambda idx_row: process_row(*idx_row), df.iterrows()), total=len(df)))


for index, news_id, keywords, date_time, author, description in results:
    df.at[index, 'News ID'] = news_id
    df.at[index, 'Keywords'] = keywords
    df.at[index, 'Date and Time'] = date_time
    df.at[index, 'Author'] = author
    df.at[index, 'Description'] = description

output_file_path = 'preprocessed_dataset.xlsx'
df.to_excel(output_file_path, index=False)
