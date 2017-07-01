#!/bin/bash

tmux new -s work -d
tmux send-keys -t work 'sudo python3 start_multiprocess_script.py' C-m
