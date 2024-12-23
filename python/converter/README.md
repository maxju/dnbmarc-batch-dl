# Setup for HTWK GPU Server:

## Activate conda environment

```bash
conda activate linus-preprocess
```

## Install dependencies

```bash
pip install -r requirements.txt
```

## Setup TMUX Bash

```bash
tmux new -s converter
```

## Run the converter with monitoring

```bash
./monitor_convert.sh
```

### keystroke to detach from tmux session

```bash
Ctrl+b d
```

### attach to tmux session

```bash
tmux attach -t converter
```

### check tmux sessions

```bash
tmux ls
```


### kill tmux session

```bash
tmux kill-session -t converter
```
