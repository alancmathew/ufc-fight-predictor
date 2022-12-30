import os
import time
import httpx
import random
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urlparse

lmap = lambda funcion, iterable: list(map(funcion, iterable))
lfilter = lambda funcion, iterable: list(filter(funcion, iterable))


dir_dict = dict()

dir_dict["data"] = "../data/"
dir_dict["raw"] = os.path.join(dir_dict["data"], "raw")
dir_dict["mid"] = os.path.join(dir_dict["data"], "mid")
dir_dict["clean"] = os.path.join(dir_dict["data"], "mid")
dir_dict["raw_csv"] = os.path.join(dir_dict["raw"], "csv")
dir_dict["raw_json"] = os.path.join(dir_dict["raw"], "json")
dir_dict["html"] = os.path.join(dir_dict["raw"], "html")
dir_dict["completed_html"] = os.path.join(dir_dict["html"], "completed")
dir_dict["upcoming_html"] = os.path.join(dir_dict["html"], "upcoming")
dir_dict["fighterlist_html"] = os.path.join(dir_dict["html"], "fighterlist")
dir_dict["fighters_html"] = os.path.join(dir_dict["html"], "fighters")
dir_dict["completed_eventlist_html"] = os.path.join(dir_dict["completed_html"], "eventlist")
dir_dict["completed_events_html"] = os.path.join(dir_dict["completed_html"], "events")
dir_dict["completed_fights_html"] = os.path.join(dir_dict["completed_html"], "fights")
dir_dict["upcoming_eventlist_html"] = os.path.join(dir_dict["upcoming_html"], "eventlist")
dir_dict["upcoming_events_html"] = os.path.join(dir_dict["upcoming_html"], "events")
dir_dict["upcoming_fights_html"] = os.path.join(dir_dict["upcoming_html"], "fights")

    
def save_html(html_str: str, filename: str, folderpath: str = dir_dict["html"]) -> None:
    os.makedirs(folderpath, exist_ok=True)
    
    filepath = os.path.join(folderpath, filename)
    with open(filepath, "w") as f:
        f.write(html_str)
        
def download_get_html(url: str, filename: str, folderpath: str = dir_dict["html"], session=None) -> str:
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
        
def download_sequential_pages(first_url: str, folderpath: str = dir_dict["html"]) -> None:
    with httpx.Client() as session:
        filename = sequential_filename_from_url(first_url)
        
        assert(filename)
        assert(first_url)
        html = download_get_html(first_url, filename, folderpath, session)
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
            inter = "&" if "?" in first_url else "?"
            for page_num in tqdm(range(2, num_pages+1)):
                if "page=1" in first_url:
                    url = first_url.replace("page=1", f"page={page_num}")
                else:
                    url = f"{first_url}{inter}page={page_num}"
                filename = sequential_filename_from_url(url)
                if not os.path.exists(os.path.join(folderpath, filename)):
                    _ = download_get_html(url, filename, folderpath, session)
                    time.sleep(random.randint(5000, 10000)/1000)
                    
                    
def save_pages(urls: list[str], folderpath: str) -> None:
    fail_score = 0
    reached_not_downloaded = False
    session = httpx.Client()
    for idx, url in enumerate(tqdm(urls)):
        if reached_not_downloaded and (idx != 0) and (idx%25 == 0):
            session.close()
            time.sleep(random.randint(60000, 300000)/1000)
            print("Switching session")
            session = httpx.Client()
        unique_id = os.path.split(url)[1]
        if not os.path.exists(os.path.join(folderpath, f"{unique_id}.html")):
            reached_not_downloaded = True
            try:
                download_get_html(url, f"{unique_id}.html", folderpath, session)
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
            
            
def write_list_to_file(thelist: list[str], filepath: str) -> None:
    with open(filepath, "w") as f:
        for item in thelist:
            f.write(f"{item}\n")
            
def read_list_from_file(filepath: str) -> list[str]:
    with open(filepath, "r") as f:
        return f.read().splitlines()

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

def main():
    for folderpath in dir_dict.values():
        os.makedirs(folderpath, exist_ok=True)
    
if __name__ == "__main__":
    main()