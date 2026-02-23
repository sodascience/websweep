from pathlib import Path
import csv
from collections import Counter
from websweep.crawler.crawler import Crawler

out=Path('/tmp/websweep_bench/tmp_crawl_test_out')
if out.exists():
    import shutil; shutil.rmtree(out)
out.mkdir(parents=True,exist_ok=True)
urls=[('https://baroque.nl','baroque'),('https://example.com','example'),('https://douglasrobson.nl','douglas')]
Crawler(target_folder_path=out,target_temp_folder_path=out,extract=True,save_html=False,overview_backend='csv',max_level=1).crawl_base_urls(urls)
with open(out/'overview_urls.tsv') as f:
    r=csv.DictReader(f,delimiter='\t')
    rows=list(r)
print(Counter(row['status'] for row in rows))
for row in rows:
    print(row['url'],row['status'])
