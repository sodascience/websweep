from pathlib import Path
import os
from info_scraper import InfoScraper
import time
import json
from datetime import date
from multiprocessing import Pool



def run_main_loop():
    start_time = time.perf_counter()
    test_data_dir = Path(__file__).parents[
                        2] / 'data' / 'scraped_data'  # Get the folder 3 folders up, then add /data/test_data to that filepath
    time_dict = {}
    json_list = []
    i = 0

    file_perm = Path(__file__).parents[2] / 'data' / 'performance_data' /  (str(date.today()) + '.json')
    Path(file_perm).parent.mkdir(parents=True, exist_ok=True)

    file_res = Path(__file__).parents[2] / 'data' / 'results' /  (str(date.today()) + '.json')
    Path(file_res).parent.mkdir(parents=True, exist_ok=True)

    # Create path to filies
    files = [os.path.join(test_data_dir, file) for file in os.listdir(test_data_dir) if not file.startswith(".")]
    # Check if they are directories
    files = [file for file in files if os.path.isdir(file)]
    
    # Parallelize loop (it may not work on Windows unless you keep "create_results" in a different file)
    with Pool() as pool, open(file_perm, "w+") as f_perm, open(file_res, "w+") as f_res:
        i = 0
        
        for result in pool.imap_unordered(create_results, files):
            i += 1
            if i % 100 == 0:
                print(f"Finished {i} files out of {len(files)}")
            time_dict_temp, json_dict = result
            time_dict.update(time_dict_temp)
            json_list.append(json_dict)

        # Write data to file  (TODO: it should be line by line to avoid using update/append. 
        # In the original code you were rewriting it at every iteration, which is not efficient)
        prettify = json.dumps(time_dict, indent=4)
        f_perm.write(prettify)

        prettify = json.dumps(json_list, indent=4)
        f_res.write(prettify)


    end_time = time.perf_counter()
    total_runtime = end_time - start_time
    time_dict["total runtime: "] = total_runtime

def create_results(path):
    folder = get_folder(path)

    start_time_file = time.perf_counter()
    website_name = os.path.basename(Path(folder).parents[0])
    cached_corporate = InfoScraper(working_dir=folder, website=website_name)
    cached_corporate.run_loops()
    end_time_file = time.perf_counter()
    json_dict = cached_corporate.__dict__
    json_dict.pop("working_dir")

    return ({folder: end_time_file - start_time_file }, json_dict)

# This evaluates all the folders in the last folder (the website folder) and opens the newest (most recent) folder
def get_folder(path):
    # Remove hidden files
    next_folder = [_ for _ in os.listdir(path) if not _.startswith(".")][0]
    final_dir = os.path.join(path, next_folder)
    
    # This should be a parameter of the code instead of finding it automatically (TODO). It could use if no parameter is passed.
    if os.path.isdir(final_dir):
        list_of_files = os.listdir(final_dir)
        list_of_files = [_ for _ in list_of_files if not _.startswith(".")]
        list_of_files.sort(reverse=True)
        return os.path.join(final_dir, list_of_files[0])
    else:
        return None



if __name__ == "__main__":
    run_main_loop()
