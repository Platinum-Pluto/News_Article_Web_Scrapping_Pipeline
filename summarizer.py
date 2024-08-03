import pandas as pd
import nltk
from nltk.tokenize import sent_tokenize
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import logging

def summarize_paragraph(paragraph):
    sentences = sent_tokenize(paragraph)
    if sentences:
        summary = sentences[0]
    else:
        summary = ""
    return summary



def apply_summarization(data):
    summaries = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(summarize_paragraph, paragraph): paragraph for paragraph in data}
        for future in tqdm(as_completed(futures), total=len(data)):
            summary = future.result()
            summaries.append(summary)
    return summaries


def add_summarizing(df):
    logging.basicConfig(level=logging.DEBUG)
    nltk.download('punkt')
    df['Summary'] = apply_summarization(df['Content'])
    print(f"Summarization completed")
    return df 
