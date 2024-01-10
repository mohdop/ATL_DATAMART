from minio import Minio
import os
import requests
import sys
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import dask.dataframe as dd

def grab_data():
    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    download_folder = "C:/Archi décisionnelle/ATL-Datamart/data/raw"

    # Create the download folder if it doesn't exist
    os.makedirs(download_folder, exist_ok=True)

    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all links on the page
        links = soup.find_all('a')

        # Iterate through the links and download the desired files
        for link in links:
            href = link.get('href')
            if href and "yellow_tripdata" in href and any(str(year) in href for year in range(2018, 2024)):
                file_url = urljoin(url, href)
                file_name = os.path.join(download_folder, href.split("/")[-1])
                print(f"Downloading {file_name}...")
                
                # Download the file
                file_content = requests.get(file_url).content
                
                # Save the file in the download folder
                with open(file_name, 'wb') as file:
                    file.write(file_content)
                
                print(f"{file_name} downloaded successfully.")
        
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")

def write_data_minio():
    """
    This method put all Parquet files into Minio
    Do not execute this method for now
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = "minio"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " already exists")

def merge_parquet_files(input_folder, output_file, chunk_size=500000):
    # List all Parquet files in the input folder
    parquet_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith('.parquet')]

    # Initialize an empty Dask DataFrame
    dfs = [dd.read_parquet(file, engine='pyarrow') for file in parquet_files]

    # Concatenate all Dask DataFrames into one
    merged_df = dd.concat(dfs, axis=0, ignore_index=True)

    # Write the merged Dask DataFrame to a single Parquet file
    merged_df.compute().to_parquet(output_file, engine='pyarrow', row_group_size=chunk_size)

def main():
    write_data_minio()
    
if __name__ == '__main__':
    main()

