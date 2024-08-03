from alright import extractor, save_data
from preprocessing import preprocess
from cleaning import cleaner
from preprocessing_1 import final_preprocessing
from summarizer import add_summarizing

def news_article_web_scrapper(input_file, initial_output_file, final_output_file):
    extract = extractor(input_file, initial_output_file)
    df = preprocess(extract)
    df = cleaner(df)
    df = final_preprocessing(df)
    df = add_summarizing(df)
    save_data(df, final_output_file)



input_file = "" 
initial_output_file = "" 
final_output_file = ""

news_article_web_scrapper(input_file, initial_output_file, final_output_file)