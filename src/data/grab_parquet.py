from minio import Minio
import urllib.request
import pandas as pd
import os
import requests
import sys
from urllib.parse import urljoin
from bs4 import BeautifulSoup

def main():
    grab_data()

def grab_data():
    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    download_folder = "C:\Archi décisionnelle/ATL-Datamart/data/raw"

    # Créer le fichier téléchargé s'il n'existe pas
    os.makedirs(download_folder, exist_ok=True)

    # Envoyer une requête GET à l'URL
    response = requests.get(url)

    # Vérifier si la requête est acceptée(status code 200)
    if response.status_code == 200:
        # Parser le contenu html de la page
        soup = BeautifulSoup(response.text, 'html.parser')

        # Trouver tous les liens dans la page
        links = soup.find_all('a')

        # boucler sur les liens et télécharger les fichiers désirés
        for link in links:
            href = link.get('href')
            if href and "yellow_tripdata" in href and any(str(year) in href for year in range(2018, 2024)):
                file_url = urljoin(url, href)
                file_name = os.path.join(download_folder, href.split("/")[-1])

                print(f"Downloading {file_name}...")

                # Télécharger le fichier
                file_content = requests.get(file_url).content

                # enregistrer le fichier dans le dossier souhaté
                with open(file_name, 'wb') as file:
                    file.write(file_content)

                print(f"{file_name} downloaded successfully.")

    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")


   

def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "NOM_DU_BUCKET_ICI"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

if __name__ == '__main__':
    sys.exit(main())
