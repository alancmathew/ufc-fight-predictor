import os
import sys
import json

def config_generator(filepath: str = "../config.json"):
    config = {
        "steps": [
            # "01_collect", 
            "02_cleanse", 
            "03_merge",
            "04_feature-eng",
            "05_agg-processor",
            "06_preprocessor",
            "07_model-selection",
            "08_model-evaluation",
            "09_feature-importances",
            "10_inference"
        ]
    }
    
    config["dirs"] = dict()
    config["dirs"]["data"] = "../data/"
    config["dirs"]["checkpoints"] = os.path.join(config["dirs"]["data"], "00_checkpoints")
    config["dirs"]["raw"] = os.path.join(config["dirs"]["data"], "01_raw")
    config["dirs"]["clean"] = os.path.join(config["dirs"]["data"], "02_cleaned")
    config["dirs"]["feature_engineered"] = os.path.join(config["dirs"]["data"], "03_feature_engineered")
    config["dirs"]["preprocessed"] = os.path.join(config["dirs"]["data"], "04_preprocessed")
    config["dirs"]["train_test"] = os.path.join(config["dirs"]["data"], "05_train_test")
    config["dirs"]["raw_csv"] = os.path.join(config["dirs"]["raw"], "csv")
    config["dirs"]["raw_json"] = os.path.join(config["dirs"]["raw"], "json")
    config["dirs"]["html"] = os.path.join(config["dirs"]["raw"], "html")
    config["dirs"]["completed_html"] = os.path.join(config["dirs"]["html"], "completed")
    config["dirs"]["upcoming_html"] = os.path.join(config["dirs"]["html"], "upcoming")
    config["dirs"]["fighterlist_html"] = os.path.join(config["dirs"]["html"], "fighterlist")
    config["dirs"]["fighters_html"] = os.path.join(config["dirs"]["html"], "fighters")
    config["dirs"]["completed_eventlist_html"] = os.path.join(config["dirs"]["completed_html"], "eventlist")
    config["dirs"]["completed_events_html"] = os.path.join(config["dirs"]["completed_html"], "events")
    config["dirs"]["completed_fights_html"] = os.path.join(config["dirs"]["completed_html"], "fights")
    config["dirs"]["upcoming_eventlist_html"] = os.path.join(config["dirs"]["upcoming_html"], "eventlist")
    config["dirs"]["upcoming_events_html"] = os.path.join(config["dirs"]["upcoming_html"], "events")
    config["dirs"]["upcoming_fights_html"] = os.path.join(config["dirs"]["upcoming_html"], "fights")
    
    with open(filepath, "w") as fh:
        json.dump(config, fh)
    
    
    
def main():
    args = sys.argv 
    if len(args) > 2:
        raise Exception("config_generator.py expects 0 or 1 arguments")
        
    filepath = args[1] if len(args) == 2 else "../config.json" 
    config_generator(filepath)
    
if __name__ == "__main__":
    main()