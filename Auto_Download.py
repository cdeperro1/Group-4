import requests
import os
from datetime import date
import pandas as pd
import glob

# Download the CSV files from the given URLs
base_url = "https://assets.ark-funds.com/fund-documents/funds-etf-csv/"
csv_filenames = ['ARK_21SHARES_BITCOIN_ETF_ARKB_HOLDINGS.csv',
                 'ARK_AUTONOMOUS_TECH._&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv',
                 'ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv',
                 'ARK_GENOMIC_REVOLUTION_ETF_ARKG_HOLDINGS.csv',
                 'ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv',
                 'ARK_ISRAEL_INNOVATIVE_TECHNOLOGY_ETF_IZRL_HOLDINGS.csv',
                 'ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv',
                 'ARK_SPACE_EXPLORATION_&_INNOVATION_ETF_ARKX_HOLDINGS.csv',
                 'THE_3D_PRINTING_ETF_PRNT_HOLDINGS.csv'
                 ]

prefix = date.today().isoformat()
prefix += '_'

# Create a directory to store the downloaded files
download_dir = 'ARK_Files'
os.makedirs(download_dir, exist_ok=True)

# Download CSV files
for filename in csv_filenames:
    url = base_url + filename
    response = requests.get(url)
    if response.status_code == 200:
        file_path = os.path.join(download_dir, prefix + filename)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded {filename}")
    else:
        print(f"Failed to download {filename}. Status code: {response.status_code}")

# Combine the downloaded CSV files into a single database
ARK_db = 'Database/ARK_database.csv'
csv_files = glob.glob('ARK_Files/*.csv')

# Initialize an empty DataFrame to hold the data
db_df = pd.DataFrame()

# Read and concatenate all CSV files
for file in csv_files:
    new_data = pd.read_csv(file)
    db_df = pd.concat([db_df, new_data], ignore_index=True)

# Remove rows with missing values
db_df.dropna(inplace=True)

# Remove duplicate rows
db_df = db_df.drop_duplicates(keep='first')

# Now, group by 'fund' and aggregate the maximum shares per fund
grouped_df = db_df.groupby('fund').agg(
    max_shares=('shares', 'max'),
    total_market_value=('market value ($)', 'sum')  # Total market value per fund
).reset_index()

# Sort the grouped data by total market value to find the top funds
sorted_grouped_df = grouped_df.sort_values(by='total_market_value', ascending=False)

# Save the combined and grouped data to a CSV
sorted_grouped_df.to_csv(ARK_db, index=False)

# Optionally, display the grouped and sorted data
print(sorted_grouped_df)

# Now let's find the top 25 companies by market value or shares within the entire dataset
top_25_df = db_df.sort_values(by='market value ($)', ascending=False).head(25)

# Save the top 25 companies to a new CSV
top_25_df.to_csv('Database/Top_25_Companies.csv', index=False)

# Display the top 25 companies
print(top_25_df)


