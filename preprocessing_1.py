import pandas as pd
import re


def final_preprocessing(df):

    # Step 2: Remove the metadata column
    if 'Metadata' in df.columns:
        df = df.drop(columns=['Metadata'])

    def remove_extra_line_spaces(text):
        return re.sub(r'\n\s*\n+', '\n', text.strip())

    if 'Content' in df.columns:
        df['Content'] = df['Content'].apply(lambda x: remove_extra_line_spaces(x) if isinstance(x, str) else x)

    print("Final preprocessing is done...")

    return df
