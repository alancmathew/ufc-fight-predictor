#!/bin/bash

config_filepath="../config.json"

source /home/$USER/miniconda3/etc/profile.d/conda.sh;
conda activate ufc-fight-predictor;

cd ./src/;
if [[ ${1} == "config" ]]; then
    python3 config_generator.py ${config_filepath};
fi
python3 runner.py ${config_filepath};
