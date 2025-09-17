#!/bin/bash

# This script multiplexes your terminal for easy navigation across multiple computers.

# Useful keyboard shortcuts within tmux:
   # `Ctrl+b, d` -- detaches from tmux
   # `Ctrl+b, [arrow_key]` -- navigates between panes (up arrow to go up, left to go left, etc.)
   # `Ctrl+b, :` -- opens the command line; the `setw synchronize-panes` command synchs and desynchs the panes

# Useful shell commands
   # `tmux attach -t [session_name]` -- attaches to the specified session
   # `tmux ls` -- lists active tmux sessions
   # `man tmux` -- opens the tmux manual

# list your hosts here, e.g., HOSTS=("carrot" "broccoli")
# visit https://www.cs.colostate.edu/machinestats/ for a list of machines you can use
HOSTS=("grouper" "denver" "moscow" "tuna" "venus" "madrid" "mars" "beijing" "turbot" "cod") # "venus" "madrid" "mars" "beijing" "turbot" "cod" "leek" "lettuce" "okra" "berlin" "yam" "cairo"

# path to your project directory, e.g., "~/cs455/hw1"
DIR="/s/chopin/g/under/camsuess/Desktop/dev/cs555/CS555-HW1"

# session name, e.g, "cs455-hw1"
SESSION="csx55-hwx"

# IT IS RECCOMMENDED THAT YOU SET UP PASSWORDLESS ssh: https://sna.cs.colostate.edu/remote-connection/ssh/keybased/

tmux kill-session -t $SESSION 2> /dev/null
tmux new-session -d -s $SESSION

FIRST_HOST=true
for HOST in "${HOSTS[@]}"
do
    if $FIRST_HOST; then
        FIRST_HOST=false
    else
        tmux split-window -t $SESSION
    fi
    tmux send-keys -t $SESSION "ssh $HOST" C-m "module load courses/cs555" C-m "cd $DIR" C-m "clear" C-m
    tmux select-layout -t $SESSION tiled
done
tmux attach -t $SESSION

