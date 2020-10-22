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
        
        self.headers = np.array([
         ("p1", "<U12"), ("p36", "i1"), 
         ("p37", "i1"), ("p2a", "<U10"),
         ("weekday(p2a)", "i1"), ("p2b", "<U4"),
         ("p6", "i1"), ("p7", "i1"),
         ("p8", "i1"), ("p9", "i1"),
         ("p10", "i1"), ("p11", "i1"),
         ("p12", "i2"), ("p13a", "i1"),
         ("p13b", "i1"), ("p13c", "i1"),
         ("p14", "i8"), ("p15", "i1"),
         ("p16", "i1"), ("p17", "i1"),
         ("p18", "i1"), ("p19", "i1"),
         ("p20", "i1"), ("p21", "i1"),
         ("p22", "i1"), ("p23", "i1"),
         ("p24", "i1"), ("p27", "i1"),
         ("p28", "i1"), ("p34", "i2"),
         ("p35", "i1"), ("p39", "i1"),
         ("p44", "i1"), ("p45a", "i1"),
         ("p47", "i1"), ("p48a", "i1"),
         ("p49", "i1"), ("p50a", "i1"),
         ("p50b", "i1"), ("p51", "i1"),
         ("p52", "i1"), ("p53", "i1"),
         ("p55a", "i1"), ("p57", "i1"),
         ("p58", "i1"),
         ("a", "i1"), ("b", "i1"),
         ("d", "i1"), ("e", "f8"),
         ("f", "f8"), ("g", "i1"),
         ("h", "i1"), ("i", "i1"),
         ("j", "i1"), ("k", "i1"),
         ("l", "i1"), ("n", "i1"),
         ("o", "i1"), ("p", "i1"),
         ("q", "i1"), ("r", "i1"),
         ("s", "i1"), ("t", "i1"),
         ("p5a", "i1")])
        self.region_cache = {}
    
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
        
        features = None
        
        for file_path in file_paths:
            archive = zipfile.ZipFile(file_paths[0], 'r')
            with archive.open(filename, "r") as region_file:
                file_features = self._parse_refion_data_from_file(region_file)
            features = self._concatenate_features(features, file_features)
            
        print(features[0].shape)
        return self.headers[...,0], features 
    
    def _parse_refion_data_from_file(self, file):
        lines_count = self._file_lines_count(file)
        file_features = [np.empty(shape=(lines_count), dtype=header[1])
                         for header in self.headers]
        reader = csv.reader(io.TextIOWrapper(file, "ISO-8859-1"), delimiter=';', quotechar='|')

        for row_index, row in enumerate(reader):
            for i in range(len(self.headers)):
                try:
                    file_features[i][row_index] = row[i]
                except ValueError:
                    file_features[i][row_index] = -1
        return file_features
      
    def _file_lines_count(self, file):
        for i, l in enumerate(file, 1):
            pass
        file.seek(0)
        return i
        
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
    
    def _concatenate_features(self, features1, features2):
        if features1 is None:
            return features2
        for i in range(len(features2)):
            features1[i] = np.concatenate([features1[i], features2[i]], axis=0)
        return features1
    
    def get_list(self, regions = None):
        if regions is None:
            regions = self.regions
            
        features = None
        
        for region in regions:
            if not region in self.regions:
                raise ValueError(F"Unknown region: {region}")
            
            region_features = self._get_region_data_from_variable(region)
            if not region_features is None:
                features = self._concatenate_features(features, region_features)
                continue
            
            region_features = self._get_region_data_from_file(region)
            if not region_features is None:
                self._save_region_data_to_variable(region, region_features)
                features = self._concatenate_features(features, region_features)
                continue
            
            _, region_features = self.parse_region_data(region)
            self._save_region_data_to_variable(region, region_features)
            self._save_region_data_to_file(region, region_features)
            features = self._concatenate_features(features, region_features)
            
        return self.headers[...,0], features
    
    def _get_region_data_from_variable(self, region):
        if region in self.region_cache:
            return self.region_cache[region]
        return None
    
    def _get_region_data_from_file(self, region):
        file_name = self.cache_filename.format(region)
        file_path = os.path.join(self.folder, file_name)
        
        if os.path.isfile(file_path):
            with open(file_path, "rb") as f:
                raw_content = f.read()
            decompressed = gzip.decompress(raw_content)
            return pickle.loads(decompressed)
        return None
    
    def _save_region_data_to_variable(self, region, region_data):
        self.region_cache[region] = region_data
    
    def _save_region_data_to_file(self, region, region_data):
        file_name = self.cache_filename.format(region)
        file_path = os.path.join(self.folder, file_name)
        
        serialized = pickle.dumps(region_data)
        compressed = gzip.compress(serialized)
        
        with open(file_path, "wb") as f:
            f.write(compressed)
    

if __name__ == "__main__":
    downloader = DataDownloader()
    h, f = downloader.get_list(["OLK", "VYS", "HKK"])
    print(h.shape, f[0].shape)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
        
           