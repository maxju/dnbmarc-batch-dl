#!/bin/bash
pip install -r requirements.txt
# Install packages from google index
pip install torch~=2.4.0 torch_xla[tpu]~=2.4.0 torchvision -f https://storage.googleapis.com/libtpu-releases/index.html
# set runtime
export PJRT_DEVICE=TPU

python convert.py

