import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

def extract_news_info(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        title = soup.find('title').text if soup.find('title') else 'No Title'
        paragraphs = soup.find_all('p')
        content = ' '.join([para.text for para in paragraphs])
        metadata = {}
        for meta in soup.find_all('meta'):
            if meta.get('name'):
                metadata[meta.get('name')] = meta.get('content')
            elif meta.get('property'):
                metadata[meta.get('property')] = meta.get('content')
        website = urlparse(url).netloc
        return url, title, content, metadata, website
    except Exception as e:
        return url, 'Error', str(e), {}, 'Error'


def extractor(file_path, output_file_path):

        checkpoint_file_path = 'Extracted-News-Articles-Checkpoint.xlsx'
        if os.path.exists(checkpoint_file_path):
            result_df = pd.read_excel(checkpoint_file_path)
            completed_urls = set(result_df['URL'])
        else:
            result_df = pd.DataFrame(columns=['URL', 'Title', 'Content', 'Metadata', 'Website'])
            completed_urls = set()

        df = pd.read_excel(file_path)
        urls_to_process = [url for url in df['URL'] if url not in completed_urls]
        data = []
        with ThreadPoolExecutor(max_workers=20) as executor:  
            future_to_url = {executor.submit(extract_news_info, url): url for url in urls_to_process}
    
            for future in tqdm(as_completed(future_to_url), total=len(future_to_url), desc="Extracting news articles"):
                url, title, content, metadata, website = future.result()
                data.append([url, title, content, metadata, website])
                if len(data) % 100 == 0:
                    temp_df = pd.DataFrame(data, columns=['URL', 'Title', 'Content', 'Metadata', 'Website'])
                    result_df = pd.concat([result_df, temp_df], ignore_index=True)
                    result_df.to_excel(checkpoint_file_path, index=False)
                    data = []

        if data:
            temp_df = pd.DataFrame(data, columns=['URL', 'Title', 'Content', 'Metadata', 'Website'])
            result_df = pd.concat([result_df, temp_df], ignore_index=True)
            result_df.to_excel(checkpoint_file_path, index=False)


        result_df.to_excel(output_file_path, index=False)
        return output_file_path


def save_data(df, output_file_path):
    df.to_excel(output_file_path, index=False)
    print(f"Final dataset file saved at {output_file_path}")
