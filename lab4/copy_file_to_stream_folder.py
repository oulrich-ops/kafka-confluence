import shutil
import time
import os
import glob


source_dir = r".\data\adidas_stream"
dest_dir = r".\data\stream"


files = glob.glob(os.path.join(source_dir, "*.csv"))

while True:
    for f in files:
        filename = os.path.basename(f)
        dest_file = os.path.join(dest_dir, filename)
        
        if not os.path.exists(dest_file):
            shutil.copy(f, dest_file)
            print(f"Copied {filename} to stream folder")
            time.sleep(1)  
