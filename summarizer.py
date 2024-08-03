import pandas as pd
import nltk
from nltk.tokenize import sent_tokenize
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.DEBUG)
nltk.download('punkt')

def summarize_paragraph(paragraph):
    sentences = sent_tokenize(paragraph)
    if sentences:
        summary = sentences[0]
    else:
        summary = ""
    return summary

file_path = 'cleaned_preprocessed_data.xlsx'
try:
    df = pd.read_excel(file_path)
except Exception as e:
    logging.error(f"Error reading Excel file: {e}")
    raise

def apply_summarization(data):
    summaries = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(summarize_paragraph, paragraph): paragraph for paragraph in data}
        for future in tqdm(as_completed(futures), total=len(data)):
            summary = future.result()
            summaries.append(summary)
    return summaries

df['Summary'] = apply_summarization(df['Content'])
output_file_path = 'cleaned_preprocessed_data_with_summary.xlsx'
df.to_excel(output_file_path, index=False)




print(f"Summarization completed. Results saved to {output_file_path}")
