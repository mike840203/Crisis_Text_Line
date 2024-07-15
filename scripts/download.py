import requests
import zipfile
import logging
import os

class Download:

    def __init__(self, unzip_path):
        self.url = "https://www.datafiles.samhsa.gov/sites/default/files/field-uploads-protected/studies/MH-CLD-2021/MH-CLD-2021-datasets/MH-CLD-2021-DS0001/MH-CLD-2021-DS0001-bundles-with-study-info/MH-CLD-2021-DS0001-bndl-data-csv_v1.zip"
        self.file_nm = self.url.split('/')[-1]
        self.unzip_path = unzip_path
        self.class_name = self.__class__.__name__

    def download_file(self):
        response = requests.get(self.url)
        with open(self.unzip_path + '/' + self.file_nm, 'wb') as file:
            file.write(response.content)
        logging.info(f"{self.class_name} - Downloaded the file to {self.unzip_path + '/' + self.file_nm}")

    def unzip_file(self):

        # Create the unzip directory if it doesn't exist
        if not os.path.exists(self.unzip_path):
            os.makedirs(self.unzip_path)

        # Unzip the file
        with zipfile.ZipFile(self.unzip_path + '/' + self.file_nm, 'r') as zip_ref:
            zip_ref.extractall(self.unzip_path)
        logging.info(f"{self.class_name} - Unzipped the file to {self.unzip_path + '/' + self.file_nm}")

    def remove_zip(self):
        os.remove(self.unzip_path + '/' + self.file_nm)
        logging.info(f"{self.class_name} - Removed the ZIP file.")

    def download(self):
        self.download_file()
        self.unzip_file()
        self.remove_zip()
        logging.info(f"{self.class_name} - Downloaded and unzipped the file.")


if __name__ == '__main__':

    raw_data_path = "../data/raw_data/mhcld_puf_2021.csv"
    unzip_path = '/'.join(raw_data_path.split('/')[:-1])
    download = Download(unzip_path)
    download.download()

