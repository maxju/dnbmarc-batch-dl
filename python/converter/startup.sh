#!/bin/bash
pip install -r requirements.txt
# Install packages from google index
pip install -f https://storage.googleapis.com/libtpu-releases/index.html
# set runtime
export PJRT_DEVICE=TPU

python convert.py
