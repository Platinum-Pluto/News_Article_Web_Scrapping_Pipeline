import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm



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


def preprocess(file_path):
    df = pd.read_excel(file_path)

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
    print("First preprocessing is done...")
    #output_file_path = 'preprocessed_dataset.xlsx'
    #df.to_excel(output_file_path, index=False)
    return df
