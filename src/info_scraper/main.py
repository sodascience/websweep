from pathlib import Path
import os
from info_scraper import InfoScraper
from timeit import timeit



def RunMainLoop():
    main_dir = Path(__file__).parents[2] #Get the folder 3 folders up,
    data_dir = main_dir / 'data' / 'test_data' #Add the 3 folders up folder with the 'data' and 'test_data' folders
    for file in os.listdir(data_dir):
        current_dir = GetDeepestFolder(data_dir, file)
        cached_corporate = InfoScraper(current_dir)
        cached_corporate.run_loops()
        print(f"alles is gedaan in {cached_corporate.working_dir}")


#recursively keep adding the directory names until the last file is no longer a directory
def GetDeepestFolder(path, file_name):
    current_dir = os.path.join(path, file_name)
    if os.path.isdir(current_dir):
        return GetDeepestFolder(current_dir, os.listdir(current_dir)[0])
    else:
        # The above recursion will always go one file to deep, so this is to reel it in once
        return Path(current_dir).parents[0]






RunMainLoop()
