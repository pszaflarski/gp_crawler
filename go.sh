#!/bin/bash

tmux new -s head -d
tmux send-keys -t work 'sudo python3 initiate_crawler_session.py' C-m
