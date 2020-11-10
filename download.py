"""
Created on Tue Oct 20 21:21:19 2020

@author: Ladislav Ondris
         xondri07@vutbr.cz
         
DataDownloader aquires PCR dataset either from web source, 
or from local cache.
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
            
        # Check whether the cache_filename is valid
        if not re.fullmatch(r"[^{}/\\]*\{\}[^{}/\\]*", cache_filename):
            raise ValueError("Invalid cache_filename parameter: " +
                             "It must contain a single pair of formatting braces {} "+
                             "and no slashes.")
            
        # Cache_filename supports only .pkl.gz
        if not cache_filename.endswith(".pkl.gz"):
            raise ValueError("Invalid cache_filename parameter: " +
                             "The only supported file type is .pkl.gz - pickle and gzip.")
            
        if not os.path.exists(folder):
            os.makedirs(folder)
            
        self.url = url
        self.folder = folder
        self.cache_filename = cache_filename
        self.regions = ["PHA","STC","JHC","PLK","ULK","HKK","JHM",
                        "MSK","OLK","ZLK","VYS","PAK","LBK","KVK",]
        self.headers = np.array([
         ("p1", "i8"), # ID
         ("p36", "i1"), # Druh pozemní komunikace 0-8 
         ("p37", "i4"), # Cislo pozemni komunikace 0-999999
         ("p2a", "datetime64[D]"), # Datum
         ("weekday(p2a)", "i1"), 
         ("p2b", "U4"), # Cas
         ("p6", "i1"), # Druh nehody 0-9
         ("p7", "i1"), # Druh srazky jedoucich vozidel 0-4
         ("p8", "i1"), # Druh pevne prekazky 0-9
         ("p9", "i1"), # Charakter nehody 1-2
         ("p10", "i1"), # 0-7
         ("p11", "i1"), # 0-9
         ("p12", "i2"), # 100-615
         ("p13a", "i1"), # usmrceno
         ("p13b", "i1"), # tezce zraneno
         ("p13c", "i1"), # lehce zraneno
         ("p14", "i4"), # hmotna skoda
         ("p15", "i1"), #
         ("p16", "i1"), #
         ("p17", "i1"), #
         ("p18", "i1"), #
         ("p19", "i1"), #
         ("p20", "i1"), #
         ("p21", "i1"), #
         ("p22", "i1"), #
         ("p23", "i1"), #
         ("p24", "i1"), 
         ("p27", "i1"),
         ("p28", "i1"), 
         ("p34", "i2"),
         ("p35", "i1"), 
         ("p39", "i1"),
         ("p44", "i1"), 
         ("p45a", "i1"),
         ("p47", "i1"), 
         ("p48a", "i1"),
         ("p49", "i1"), 
         ("p50a", "i1"),
         ("p50b", "i1"),
         ("p51", "i1"),
         ("p52", "i1"), 
         ("p53", "i4"),
         ("p55a", "i1"), 
         ("p57", "i1"),
         ("p58", "i1"),
         ("a", "f8"), ("b", "f8"),
         ("d", "f8"), ("e", "f8"), # Souradnice
         ("f", "f8"), 
         ("g", "f8"),
         ("h", "U50"), 
         ("i", "U16"),
         ("j", "U16"), 
         ("k", "U16"), # Typ silnice - dalnice, 1. tridy,...
         ("l", "U8"), # ID silnice
         ("n", "i4"),
         ("o", "f8"), 
         ("p", "U22"), # Opačnýkesměruúseku, Souhlasnýsesměremúseku
         ("q", "U17"), # 'Odbočovacívpravo' 'Pomalý' 'Připojovacívpravo' 'Rychlý'
         ("r", "i4"),
         ("s", "i4"), 
         ("t", "U32"), # 'GN_V0.1UIR-ADR_410' 'SN_20050929UIR-ADR_410' 'ULS_20050701UIR-ADR_410'
         ("p5a", "i1"), 
         ("region", "U3")])
        self.region_cache = {}
    
    def download_data(self):
        """
        Requests the page, finds all available zips 
        and downloads only the relevant ones into self.folder.
        """
        html_page = self._request_html_page()
        hrefs = self._get_zip_hrefs(html_page)
        urls_and_paths= self._get_urls_and_paths(hrefs)
        
        for url, path in urls_and_paths:
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
        filtered_hrefs = self._get_latest_paths_for_each_year(hrefs)
        
        for href in filtered_hrefs:
            file_name = Path(href).name
            file_url = urllib.parse.urljoin(self.url, href)
            file_path = os.path.join(self.folder, file_name)
            yield file_url, file_path
        
    def _get_latest_paths_for_each_year(self, file_paths):
        file_names = np.array([Path(path).name for path in file_paths])
        grouped_files = self._group_files_by_year(file_names, file_paths)
       
        latest_paths = []
        for file_names, file_paths, year in grouped_files:
            latest_path = self._get_latest_path_in_year(file_names, file_paths, year)
            if not latest_path is None:
                latest_paths.append(latest_path)
        return latest_paths
                
    def _group_files_by_year(self, file_names, file_paths):
        file_paths = np.array(file_paths)
        years = np.zeros(shape=(len(file_paths)), dtype=int)
        year_regex = re.compile(".*(\d{4})\.zip")
        
        for i, file_name in enumerate(file_names):
            match = year_regex.match(file_name)
            if match:
                years[i] = match.group(1)
        
        years_unique, years_indices = np.unique(years, return_inverse=True)
        
        for i, year in enumerate(years_unique):
            if year == 0:
                continue
            
            year_indices = np.squeeze(np.argwhere(years_indices == i))
            year_file_names = file_names[year_indices].flatten()
            year_file_paths = file_paths[year_indices].flatten()
            yield year_file_names, year_file_paths, year

    def _get_latest_path_in_year(self, file_names, file_paths, year):
        latest_month = 0
        latest_file_path = None
        
        month_regex = re.compile(fr".*(\d\d)-{year}\.zip")
        for i, file_name in enumerate(file_names):
            match = month_regex.match(file_name)
            
            if not match:
                return file_paths[i]
            
            matched_month = int(match.group(1))
            if matched_month > latest_month:
                latest_month = matched_month
                latest_file_path = file_paths[i]
        return latest_file_path
            
    def _download_file(self, source_url, save_file_path):
        request = requests.get(source_url, stream=True)
        with open(save_file_path, 'wb') as fd:
            for chunk in request.iter_content(chunk_size=128):
                fd.write(chunk)
    
    def parse_region_data(self, region):
        """
        Given a region name, it parses information about the region 
        from zip files in self.folder. If a file is missing, it is downloaded.

        Parameters
        ----------
        region : string
            A three-character abbreviation of a region.

        Returns
        -------
        2-D Tuple
            Returns a tuple containing header names and a list of numpy arrays.

        """
        filename = self._try_convert_region_to_filename(region)
        self._download_files_if_not_exist()
        file_paths = self._get_data_file_paths()
        file_paths = self._get_latest_paths_for_each_year(file_paths)
        
        features = None
        for file_path in file_paths:
            archive = zipfile.ZipFile(file_path, 'r')
            with archive.open(filename, "r") as region_file:
                file_features = self._parse_region_data_from_file(region_file)
                file_features[-1][...] = region
            features = self._concatenate_features(features, file_features)
        
        return self.headers[...,0], features 
    
    def _get_data_file_paths(self):
        return glob.glob(os.path.join(self.folder, "*.zip"))
        
    def _parse_region_data_from_file(self, file):
        lines_count = self._file_lines_count(file)
        file_features = self._create_empty_arrays(lines_count)
        reader = csv.reader(io.TextIOWrapper(file, "Windows-1250"), delimiter=';', quotechar='"')

        for row_index, row in enumerate(reader):
            feature_col = 0
            for i in range(len(row)): # For each column
                if self.headers[i][1] == "f8":
                    row[i] = row[i].replace(',', '.')
                try:
                    file_features[feature_col][row_index] = row[i]
                except ValueError:
                    pass
                feature_col += 1
            
        return file_features
      
    def _file_lines_count(self, file):
        for i, l in enumerate(file, 1):
            pass
        file.seek(0)
        return i
    
    def _create_empty_arrays(self, lines_count):
        file_features = []
        for header_name, header_type in self.headers:
            if header_type == "f8":
                fill_value = np.nan
            else:
                fill_value = -1
            ndarray = np.full(shape=(lines_count), fill_value=fill_value, dtype=header_type)
            file_features.append(ndarray)
        return file_features
    
    def _download_files_if_not_exist(self):
        html_page = self._request_html_page()
        hrefs = self._get_zip_hrefs(html_page)
        urls_and_paths = self._get_urls_and_paths(hrefs)
        
        downloaded_count = 0
        for url, path in urls_and_paths:
            if not os.path.isfile(path):
                self._download_file(url, path)
                downloaded_count += 1
        return downloaded_count
    
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
        """
        Returns information about accidents for specified regions. 
        First, it tries to find the information in a cache variable, then in a directory from files,
        and lastly, it calls parse_region_data to retrieve the information for a particular
        region.
        

        Parameters
        ----------
        regions : list of strings, optional
            The list of regions to retrieve information about.
            The default is None. If None, all regions are selected.

        Raises
        ------
        ValueError
            Raises ValueError if an unknown region is requested..

        Returns
        -------
        2-D Tuple
            Returns a tuple containing header names and a list of numpy arrays.
        """
        if regions is None:
            regions = self.regions
            
        downloaded = self._download_files_if_not_exist()
        if downloaded > 0: # if a new file is downloaded, delete all cache
            self._clear_cache()
        
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
    
    def _clear_cache(self):
        self.region_cache.clear()
        files = glob.glob(os.path.join(self.folder, self.cache_filename.format('*')))
        for local_file_cache in files:
            os.remove(local_file_cache)
        
    
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

def print_unique(ar):
    u = np.sort(np.unique(ar))
    print("Unique:", u.shape, u)

if __name__ == "__main__":
    downloader = DataDownloader()
    h, f = downloader.get_list(["OLK", "VYS", "HKK"])
    print("Regions:", np.unique(f[64]))
    print("Number of records:", f[0].shape[0])
    print("Headers count:", len(h))
    print("Headers:", h)
    
    # Use the following lines to measure the time taken by calling the get_list method
    """
    start = time.time()
    downloader.get_list(["OLK"])
    end = time.time()
    print(end - start)
    """
    
    # Use the following lines to print unique values in the column
    #h, f = downloader.parse_region_data("ULK")
    #header_index = np.argwhere(downloader.headers[...,0] == "region").flatten()[0]
    #print_unique(f[header_index])
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
        
           