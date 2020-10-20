"""
Created on Tue Oct 20 21:21:19 2020

@author: Ladislav Ondris (xondri07)
         ladislav.ondris.1@gmail.com
"""

import numpy as np
import requests
import urllib 
import re
import os
import io
import glob
import gzip, pickle, csv, zipfile
from bs4 import BeautifulSoup
from pathlib import Path


class DataDownloader():
    def __init__(self, url="https://ehw.fit.vutbr.cz/izv/",
                 folder="data", cache_filename="data_{}.pkl.gz"):
        
        if url == "":
            raise ValueError("Url cannot be empty.")
            
        if not cache_filename.endswith(".pkl.gz"):
            raise ValueError("The only supported file type is .pkl.gz - pickle and gzip.")
            
        if not os.path.exists(folder):
            os.makedirs(folder)
            
        self.url = url
        self.folder = folder
        self.cache_filename = cache_filename
        self.regions = ["PHA","STC","JHC","PLK","ULK","HKK","JHM",
                        "MSK","OLK","ZLK","VYS","PAK","LBK","KVK",]
    
    
    def download_data(self):
        html_page = self._request_html_page()
        hrefs = self._get_zip_hrefs(html_page)
        urls, paths = self._get_urls_and_paths(hrefs)
        for url, path in zip(urls, paths):
            self._download_file(url, path)
        
    def _request_html_page(self):
        cookies = {
            '_ranaCid': '1991771124.1594660423',
            '_ga': 'GA1.2.1948604486.1594660423',
            '_gcl_au': '1.1.733078043.1602714835',
        }
        
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36 OPR/71.0.3770.271',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        
        response = requests.get('https://ehw.fit.vutbr.cz/izv/', headers=headers, cookies=cookies).text
        return response
    
    def _get_zip_hrefs(self, html):
        soup = BeautifulSoup(html, "html.parser")
        hrefs = [link.get('href') for link in soup.findAll('a')]
        valid_hrefs = [href for href in hrefs if href.endswith(".zip")]
        return valid_hrefs
    
    def _get_urls_and_paths(self, hrefs):
        for href in hrefs:
            file_name = Path(href).name
            file_url = urllib.parse.urljoin(self.url, href)
            file_path = os.path.join(self.folder, file_name)
            yield file_url, file_path
            
    def _download_file(self, source_url, save_file_path):
        request = requests.get(source_url, stream=True)
        with open(save_file_path, 'wb') as fd:
            for chunk in request.iter_content(chunk_size=128):
                fd.write(chunk)
    
    def _get_data_file_paths(self, ):
        return glob.glob(os.path.join(self.folder, "*.zip"))
        
    def parse_region_data(self, region):
        filename = self._convert_region_to_filename(region)
        #self._download_files_if_not_exist()
        file_paths = self._get_data_file_paths()
        
        feature_headers = []
        features = []
        
        for file_path in file_paths:
            archive = zipfile.ZipFile(file_paths[0], 'r')
            with archive.open(filename, "r") as region_file:
                content = self._preprocesses_file_content(region_file)
            break
            
    def _preprocesses_file_content(self, file):
        reader = csv.reader(io.TextIOWrapper(file, "ISO-8859-1"), delimiter=';', quotechar='|')
        rows = list(reader)
        print(len(rows))
        #for row in reader:
        #    print(row)
        #    break
        #print(lines[0], len(lines))
      
    def _download_files_if_not_exist(self):
        html_page = self._request_html_page()
        hrefs = self._get_zip_hrefs(html_page)
        urls, paths = self._get_urls_and_paths(hrefs)
        
        for url, path in zip(urls, paths):
            if not os.path.isfile(path):
                self._download_file(url, path)
    
    def _try_convert_region_to_filename(self, region):
        try:
            return self._convert_region_to_filename(region)
        except ValueError:
            raise ValueError("Unknown region: {region}")
    
    def _convert_region_to_filename(self, region):
        index = self.regions.index(region)
        if index > 7:
            index += 6
        return F"{index:02.0f}.csv"
        

if __name__ == "__main__":
    downloader = DataDownloader()
    downloader.parse_region_data("OLK")
        
           