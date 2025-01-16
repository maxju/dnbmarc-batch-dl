# MARC Record Converter Setup (HTWK GPU Server)

## Setup and Running

1. Navigate to the converter directory:
```bash
cd python/converter
```

2. Create and attach to tmux session:
```bash
tmux new -s converter
```

3. Inside tmux, perform the setup:
```bash
conda activate linus-preprocess
pip install -r requirements.txt
```

4. Start the converter with monitoring (still inside tmux):
```bash
./monitor_convert.sh
```

## TMUX Commands Reference

All these commands can be used to manage your tmux session:

- Detach from session: `Ctrl+b d`
- Reattach to session: `tmux attach -t converter`
- List all sessions: `tmux ls`
- Kill session: `tmux kill-session -t converter`
