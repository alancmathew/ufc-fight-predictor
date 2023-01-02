import os
import time
import json
import httpx
import random
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urlparse

lmap = lambda funcion, iterable: list(map(funcion, iterable))
lfilter = lambda funcion, iterable: list(filter(funcion, iterable))

dir_dict = dict()

def write_list_to_file(thelist: list[str], filepath: str) -> None:
    with open(filepath, "w") as f:
        for item in thelist:
            f.write(f"{item}\n")
            
def read_list_from_file(filepath: str) -> list[str]:
    with open(filepath, "r") as f:
        return f.read().splitlines()

    
def save_html(html_str: str, filename: str, folderpath: str) -> None:
    os.makedirs(folderpath, exist_ok=True)
    
    filepath = os.path.join(folderpath, filename)
    with open(filepath, "w") as f:
        f.write(html_str)
        
def download_get_html(url: str, filename: str, folderpath: str, session=None) -> str:
    if session:
        r = session.get(url)
    else:
        r = httpx.get(url)
        
    if r.status_code == 200:
        html_str = r.text
        save_html(html_str, filename, folderpath)
        return html_str
    else:
        print(f"Unable to get: {url}")

def sequential_filename_from_url(url):
    parsed = urlparse(url)
    
    query = parsed.query
    if query:
        filename = ""
        parts = query.split('&')
        for part in parts:
            var, val = part.split('=')
            if val.isnumeric():
                val = f"{int(val):02}"

            if filename:
                filename = f"{filename}_{var}-{val}"
            else:
                filename = f"{var}-{val}"

        filename = f"{filename}.html"
        
    else:
        filename = "page_01.html"

    return filename
                    
                    
def save_pages(urls: list[str], folderpath: str, filenames: list[str] = None) -> None:
    fail_score = 0
    reached_not_downloaded = False
    session = httpx.Client()
    iterator = zip(urls, filenames) if filenames else urls
    for idx, val in enumerate(tqdm(iterator)):
        if reached_not_downloaded and (idx != 0) and (idx%25 == 0):
            session.close()
            time.sleep(random.randint(60000, 300000)/1000)
            print("Switching session")
            session = httpx.Client()
            
        if len(val) == 2:
            url, filename = val
        else:
            url = val
            unique_id = os.path.split(url)[1]
            filename = f"{unique_id}.html"
        if not os.path.exists(os.path.join(folderpath, filename)):
            reached_not_downloaded = True
            try:
                download_get_html(url, filename, folderpath, session)
                if fail_score > 0:
                    fail_score -= 1
            except:
                fail_score += 2
                print(f"Network request unsuccessful. Fail score: {fail_score}")
                session.close()
                
                if fail_score >= 20:
                    break
                session = httpx.Client()
            finally:
                time.sleep(random.randint(10000, 20000)/1000)
        else:
            reached_not_downloaded = False
            
            
def download_sequential_pages(first_url: str, folderpath: str) -> None:
    
    filename = sequential_filename_from_url(first_url)

    assert(filename)
    assert(first_url)
    filepath = os.path.join(folderpath, filename)
    if not os.path.exists(filepath):
        html = download_get_html(first_url, filename, folderpath)
    else:
        with open(filepath, "r") as fh:
            html = fh.read()
            
    soup = BeautifulSoup(html, features="lxml")

    page_links = soup.find_all("a", class_="b-statistics__paginate-link")

    page_nums = [a.text.strip() for a in page_links]

    for idx, num in enumerate(page_nums):
        try:
            page_nums[idx] = int(num)
        except ValueError:
            page_nums.remove(num)

    num_pages = max(page_nums)

    if num_pages != 1:
        urls, filenames = [], []
        inter = "&" if "?" in first_url else "?"
        for page_num in range(2, num_pages+1):
            if "page=1" in first_url:
                url = first_url.replace("page=1", f"page={page_num}")
            else:
                url = f"{first_url}{inter}page={page_num}"
            filename = sequential_filename_from_url(url)
            urls.append(url)
            filenames.append(filename)
            
        save_pages(urls, folderpath, filenames)
                    

def format_filename(file):
    filename = file.replace(".html", "")
    newfilename = ""
    parts = filename.split("-")
    for part in parts:
        if part[:4] == "char":
            newfilename = f"char-{part[4]}"
        for i, c in enumerate(part):
            if c.isnumeric():
                if newfilename:
                    newfilename = f"{newfilename}_{part[:i]}-{part[i:]}"
                else:
                    newfilename = f"{part[:i]}-{part[i:]}"
                break
                
    return f"{newfilename}.html"


def format_sequential_filenames(folderpath):
    files = sorted(lfilter(lambda s: s.endswith(".html"), \
                       os.listdir(folderpath)))
    
    for file in tqdm(files):
        filepath = os.path.join(folderpath, file)
        with open(filepath, 'r') as f:
            content = f.read()

        newfile = format_filename(file)
        newfilepath = os.path.join(folderpath, newfile)
        with open(newfilepath, 'w') as f:
            f.write(content)

        os.remove(filepath)
        
        
class ColumnTracker:
    def __init__(self, init_list):
        self.main_list = init_list
        self.print_num_left()
        
    def remove(self, to_remove):
        if not isinstance(to_remove, list):
            to_remove = [to_remove]
            
        for col in to_remove:
            if col in self.main_list:
                self.main_list.remove(col)
            
        self.print_num_left()
        
    def list_left(self):
        return self.main_list
    
    def num_left(self):
        return len(self.main_list)
    
    def print_num_left(self):
        print(f"Remaining columns: {len(self.main_list)}")
        

def main():
    
    global dir_dict
    with open("../config.json", "r") as fh:
        dir_dict = json.load(fh)["dirs"]
    
    for folderpath in dir_dict.values():
        os.makedirs(folderpath, exist_ok=True)

        
main()