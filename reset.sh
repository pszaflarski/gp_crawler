#!/bin/bash

tmux kill-session -t work

tmux new -s work -d
tmux send-keys -t work 'sudo python3 start_multiprocess_script.py' C-m
