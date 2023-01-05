import os
import sys
import json
import importlib
import subprocess


def convert_nb_to_py(steps):
    cmd = "cd ../nbs/"
    for step in steps:
        cmd = f"{cmd} && jupyter nbconvert {step}.ipynb --to script"
    cmd = f"{cmd} && mv *.py ../src/"
    os.system(cmd)

    
def run_pipeline(steps):
    for step in steps:
        func = importlib.import_module(step).main
        func()
    

def main():
    args = sys.argv
    if len(args) != 2:
        raise("runner.py requires filepath of config.json as argument")
        
    with open(args[1], "r") as fh:
        config = json.load(fh)
        
    steps = config["steps"]
    convert_nb_to_py(steps)
    run_pipeline(steps)
    
    
if __name__ == "__main__":
    main()