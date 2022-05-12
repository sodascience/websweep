from pathlib import Path
import os
from info_scraper import InfoScraper
import time
import json
from datetime import date


def RunMainLoop():
    start_time = time.perf_counter()
    test_data_dir = Path(__file__).parents[
                        2] / 'data' / 'test_data'  # Get the folder 3 folders up, then add /data/test_data to that filepath
    time_dict = {}
    json_list = []
    i = 0
    for file in os.listdir(test_data_dir):
        if i == 50:
            break
        i += 1
        path = os.path.join(test_data_dir, file)
        folder = GetFolder(path)
        start_time_file = time.perf_counter()
        website_name = os.path.basename(Path(folder).parents[0])
        cached_corporate = InfoScraper(working_dir=folder, website=website_name)
        cached_corporate.run_loops()
        end_time_file = time.perf_counter()
        time_dict[folder] = end_time_file - start_time_file
        json_dict = cached_corporate.__dict__
        json_dict.pop("working_dir")
        json_list.append(json_dict)
        SavePerformance(time_dict)
        SaveJson(json_list)
    end_time = time.perf_counter()
    total_runtime = end_time - start_time
    time_dict["total runtime: "] = total_runtime



# This evaluates all the folders in the last folder (the website folder) and opens the newest (most recent) folder
def GetFolder(path):
    next_folder = os.listdir(path)[0]
    final_dir = os.path.join(path, next_folder)
    list_of_files = os.listdir(final_dir)
    list_of_files.sort(reverse=True)
    return os.path.join(final_dir, list_of_files[0])


def SavePerformance(performance):
    prettify = json.dumps(performance, indent=4)
    file_path = Path(__file__).parents[2] / 'data' / 'performance_data'
    file_name = '\\' + str(date.today()) + '.json'
    saved_json = str(file_path) + file_name
    file = open(saved_json, 'w')
    file.write(prettify)
    file.close()


def SaveJson(json_list):
    prettify = json.dumps(json_list, indent=4)
    file_path = Path(__file__).parents[2] / 'data' / 'results'
    file_name = '\\' + str(date.today()) + '.json'
    saved_json = str(file_path) + file_name
    file = open(saved_json, 'w')
    file.write(prettify)
    file.close()


if __name__ == "__main__":
    RunMainLoop()
