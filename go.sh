#!/bin/bash

tmux new -s head -d
tmux send-keys -t head 'sudo python3 initiate_crawler_session.py' C-m

tmux new -s work -d
tmux send-keys -t work 'sudo python3 start_multiprocess_script.py' C-m
