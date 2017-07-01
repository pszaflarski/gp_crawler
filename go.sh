#!/bin/bash

tmux new -s head -d
tmux send-keys -t head 'sudo python3 initiate_crawler_session.py' C-m
