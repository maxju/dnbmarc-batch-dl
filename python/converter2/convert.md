1) Request Ressources
srun --pty --ntasks=1 --cpus-per-task=4 --gres=gpu:4 --time=1:00:00 --mem-per-cpu=8192 bash -l

2) set environment variables and run converter
NUM_DEVICES=4 NUM_WORKERS=18 MAX_FILES=100 ./marker_chunk_convert.sh ./data/pdf/ ./data/md/

