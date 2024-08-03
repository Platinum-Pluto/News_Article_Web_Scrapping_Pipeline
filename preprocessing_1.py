import pandas as pd
import re

file_path = 'cleaned_preprocessed_data.xlsx' 
df = pd.read_excel(file_path)

# Step 2: Remove the metadata column
if 'Metadata' in df.columns:
    df = df.drop(columns=['Metadata'])

def remove_extra_line_spaces(text):
    return re.sub(r'\n\s*\n+', '\n', text.strip())

if 'Content' in df.columns:
    df['Content'] = df['Content'].apply(lambda x: remove_extra_line_spaces(x) if isinstance(x, str) else x)


output_file_path = 'cleaned_preprocessed_data.xlsx' 
df.to_excel(output_file_path, index=False)

print("Excel file processed and saved successfully.")

print(df)
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import re
# Load the dataset
file_path = 'cleaned_preprocessed_data.xlsx'  # Update with your file path
df = pd.read_excel(file_path)

# Generate summary statistics
summary_statistics = df.describe(include='all')

# Generate report
report = []
report.append("Dataset Summary Report\n")
report.append("======================\n")
report.append(f"Number of rows: {df.shape[0]}\n")
report.append(f"Number of columns: {df.shape[1]}\n\n")
report.append("Summary Statistics:\n")
report.append(summary_statistics.to_string())
report.append("\n\n")

# Check for missing values
missing_values = df.isnull().sum()
report.append("Missing Values:\n")
report.append(missing_values.to_string())
report.append("\n\n")

# Visualizations
output_dir = 'report_images'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Example visualization: Distribution of numerical columns
for column in df.select_dtypes(include='number').columns:
    plt.figure()
    sns.histplot(df[column].dropna(), kde=True)
    plt.title(f'Distribution of {column}')
    plt.savefig(f'{output_dir}/{column}_distribution.png')
    plt.close()

# Save report to a text file
report_file_path = 'dataset_report.txt'
with open(report_file_path, 'w') as file:
    file.write('\n'.join(report))

print("Report generated successfully.")
"""