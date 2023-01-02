#!/bin/bash

source /home/$USER/miniconda3/etc/profile.d/conda.sh;
conda activate ufc-fight-predictor;

cd ./src/ && python3 config_generator.py && python3 runner.py;