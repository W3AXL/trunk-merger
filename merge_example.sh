#!/bin/bash

# Timestamp used for process logging
timestamp=`date '+%Y-%m-%d_%H%M%S'`
# Folder of input trunk-recorder calls (will be crawled recursively)
infolder="/mnt/example/in"
# Output folder to save merged clips to (will be auto-organized by talkgroup and time/date)
outfolder="/mnt/example/merged"
# Talkgroups file from trunk-recorder
trunkfile="/mnt/example/talkgroups.csv"
# Minimum priority to merge & archive
priority=3
# Max number of threads to use while merging
threads=4

screen -dmS "trunk-merger" -L -Logfile "logs/trunk-merger_$timestamp.log" python merger.py -t $trunkfile -i $infolder -o $outfolder -p $priority -n $threads -rm --normalize
