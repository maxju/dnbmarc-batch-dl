#!/bin/bash
git clone -b converter https://github.com/maxju/dnbmarc-batch-dl.git
cd dnbmarc-batch-dl/python/converter
pip install -r requirements.txt
# Install packages from google index
pip install -f https://storage.googleapis.com/libtpu-releases/index.html
# set runtime
export PJRT_DEVICE=TPU

python convert.py
