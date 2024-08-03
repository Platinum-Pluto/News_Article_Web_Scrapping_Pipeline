import pandas as pd
import janitor
import random
import string

# Function to generate a unique 8-digit random number
def generate_unique_id(existing_ids):
    while True:
        random_id = ''.join(random.choices(string.digits, k=8))
        if random_id not in existing_ids and random_id[0] != '0':
            existing_ids.add(random_id)  # Add the new ID to the set
            return random_id

# Load the Excel file
file_path = 'preprocessed_dataset.xlsx'
df = pd.read_excel(file_path)

# Create an empty set for existing IDs
existing_ids = set()
existing_ids.update(df['News ID'].astype(str).unique())
# Using pyjanitor chain for data cleaning and preprocessing
df_cleaned = (
    df
    # Step 1: Replace news ID column with unique 8-digit random numbers
    .assign(News_ID=lambda df: df['News ID'].apply(lambda _: generate_unique_id(existing_ids)))
    # Step 2: Remove rows which contain the word "Error" in the Title column
    .loc[~df['Title'].str.contains('Error', na=False)]
    # Step 3: Remove rows which do not contain anything in the Description column
    .dropna(subset=['Description'])
    .dropna(subset=['Keywords'])
    # Step 4: Remove rows which do not contain anything in the Keywords column and Description column together
    .dropna(subset=['Description', 'Keywords'], how='all')
    # Step 5: Remove rows which contain "Please enable JS and disable any ad blocker" in the Content column
    .loc[~df['Content'].str.contains('Please enable JS and disable any ad blocker', na=False)]
    .loc[~df['Content'].str.contains('Advertisement', na=False)]
    # Step 6: Remove ad related rows in the Metadata column
   # .loc[~df['Metadata'].str.contains('ad', na=False, case=False)]
   # .drop(columns=['News ID'], inplace=True)
)

print(df_cleaned)
# Save the cleaned data to a new Excel file
cleaned_file_path = 'cleaned_preprocessed_data.xlsx'
df_cleaned.to_excel(cleaned_file_path, index=False)

print(f"Data preprocessing and cleaning completed. Cleaned data saved to {cleaned_file_path}")



# Load the Excel file
file_path = 'cleaned_preprocessed_data.xlsx'
df = pd.read_excel(file_path)
print(df)

# Drop the second "News ID" column
df.drop(columns=['News ID'], inplace=True)
# Save the cleaned data to a new Excel file
print(df)
cleaned_file_path = 'cleaned_preprocessed_data.xlsx'
df.to_excel(cleaned_file_path, index=False)

print(f"Data preprocessing and cleaning completed. Cleaned data saved to {cleaned_file_path}")










