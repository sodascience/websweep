from pathlib import Path
import os
from info_scraper import InfoScraper
import time
import glob

def RunMainLoop():
    start_time = time.perf_counter()
    test_data_dir = Path(__file__).parents[2] / 'data' / 'test_data'  # Get the folder 3 folders up, then add /data/test_data to that filepath
    time_dict = {}
    for file in os.listdir(test_data_dir):
        path = os.path.join(test_data_dir, file)
        folder = GetFolder(path)
        start_time_file = time.perf_counter()
        cached_corporate = InfoScraper(working_dir=folder)
        cached_corporate.run_loops()
        end_time_file = time.perf_counter()
        time_dict[folder] = end_time_file - start_time_file
        #print(f"alles is gedaan in {cached_corporate.working_dir}")
    end_time = time.perf_counter()
    print(f"Total execution time is {end_time - start_time}, met deze spreid {time_dict}")

# This evaluates all the folders in the last folder (the website folder) and opens the newest (most recent) folder
def GetFolder(path):
    next_folder = os.listdir(path)[0]
    final_dir = os.path.join(path, next_folder)
    list_of_files = os.listdir(final_dir)
    list_of_files.sort(reverse=True)
    return os.path.join(final_dir, list_of_files[0])

if __name__ == "__main__":
    RunMainLoop()


