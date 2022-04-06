# trunk-merger
Small python utility to take trunk-recorder input files and merge them into 30-minute archived recordings, like you'd get from Broadcastify or similar sources.

```
~/trunk-merger$ python merger.py -h
usage: merger.py [-h] [-t tg.csv] [-i ~/recordings/] [-o ~/archive/] [-n 4] [-p 3] [-rm] [-e] [-v]

Combines individual trunk-recorder talkgroup recordings into time-accurate archives

optional arguments:
  -h, --help            show this help message and exit
  -t tg.csv, --trunk-file tg.csv
                        trunk-recorder talkgroup CSV file
  -i ~/recordings/, --input ~/recordings/
                        trunk-recorder archive folder (will descend recursively)
  -o ~/archive/, --output ~/archive/
                        output directory for combined archive recordings (default = current folder)
  -n 4, --num-threads 4
                        number of concurrent processing threads (default = 4)
  -p 3, --priority 3    Lowest priority talkgroup to combine archives for (default = 3)
  -rm, --remove         remove input files once combined
  -e, --keep-empty      keep empty output audio files
  -v, --verbose         enable verbose logging
```
