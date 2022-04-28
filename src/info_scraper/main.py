from pathlib import Path
import os
import glob as glob
from info_scraper import InfoScraper




def RunMainLoop():
    main_dir = Path(__file__).parents[2] #Get the folder 3 folders up,
    data_dir = main_dir / 'data' / 'test_data' #Add the 3 folders up folder with the 'data' and 'test_data' folders
    for file in os.listdir(data_dir):
        current_dir = os.path.join(data_dir, file)
        #recursively keep adding the directory names until the last file is no longer a directory
        while os.path.isdir(current_dir):
            current_dir = AddFileNameToPath(current_dir, os.listdir(current_dir)[0])

        current_dir = Path(current_dir).parents[0] #The above recursion will always go one file to deep, so this is to reel it in once
        cached_corporate = InfoScraper(current_dir)
        for to_be_scraped_file in os.listdir(cached_corporate.working_dir):
            cached_corporate.run_loops(to_be_scraped_file)
        print(f"alles is gedaan in {current_dir}")



def AddFileNameToPath(path, file_name):
    return os.path.join(path, file_name)





print("Lets Start")
input()
RunMainLoop()
