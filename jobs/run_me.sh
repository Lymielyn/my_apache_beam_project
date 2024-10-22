#!/bin/bash
pip install -r ../containers/requirements.txt 
python direct_runner_transactions.py
python unittesting.py