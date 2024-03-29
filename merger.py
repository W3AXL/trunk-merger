#!/usr/bin/env python

"""
-----------------------------------------------
    Audio Merger Script for Trunk-Recorder

    Combines individual talkgroup recordings into combined, properly time-gapped files
"""

from contextlib import nullcontext
from curses import meta
from datetime import datetime, timedelta
import time
import argparse
import logging
import os
import sys
import csv

from multiprocessing import Pool
from multiprocessing import freeze_support
from xmlrpc.client import DateTime

from pydub import AudioSegment, effects
import pydub.exceptions as pydubex

# User Variables
trunkCsv = None
recPath = None
outPath = os.path.abspath('./')
numThreads = 4
priority = 3
remove = False
keepEmpty = False

# Global Variables
talkgroups = []     # pairs of [tgId, tgTag] entries

# Audio configuration
samplerate = 24000
bitrate = 18

# Function Defs

def datetimeRange(start, end, delta):
    """
    Generate an array of datetime objects, between the start and end dates and spaced by delta

    Args:
        start (datetime): starting date
        end (datetime): ending date
        delta (timedelta): spacing between objects in list
    """
    current = start
    while current < end:
        yield current
        current += delta

def datetimeFloor(dt, delta):
    """
    Round datetime down to nearest delta increment

    Args:
        dt (datetime): date object to round
        delta (_type_): timedelta to round to
    """
    return dt - (dt - datetime.min) % delta

def datetimeCeil(dt, delta):
    """
    Round datetime up to nearest delta increment

    Args:
        dt (datetime): date object to round
        delta (_type_): timedelta to round to
    """
    return dt + (datetime.min - dt) % delta

def parseArgs():
    """
    Configures and parses command line arguments
    """

    global trunkCsv
    global recPath
    global numThreads
    global outPath
    global priority
    global remove
    global keepEmpty
    global norm
    
    parser = argparse.ArgumentParser(description="Combines individual trunk-recorder talkgroup recordings into time-accurate archives")
    
    parser.add_argument('-t', '--trunk-file', metavar="tg.csv", help="trunk-recorder talkgroup CSV file", required=True)
    parser.add_argument('-i', '--input', metavar="~/recordings/", help="trunk-recorder archive folder (will descend recursively)", required=True)
    parser.add_argument('-o', '--output', metavar="~/archive/", help="output directory for combined archive recordings (default = current folder)")
    parser.add_argument('-n', '--num-threads', metavar="4", help="number of concurrent processing threads (default = 4)")
    parser.add_argument('-p', '--priority', metavar='3', help="Lowest priority talkgroup to combine archives for (default = 3)")
    parser.add_argument('-rm', '--remove', action='store_true', help='remove input files once combined')
    parser.add_argument('-e', '--keep-empty', action='store_true', help='keep empty output audio files')
    parser.add_argument('--normalize', action='store_true', help='normalize audio files')
    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose logging')

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
        logging.debug("Enabled verbose logging")
    else:
        logging.basicConfig(level=logging.INFO)

    if args.trunk_file:
        trunkCsv = args.trunk_file
    else:
        logging.error("No trunking CSV specified, exiting")
        exit(1)
    
    if args.input:
        recPath = args.input
    else:
        logging.error("No recording folder specified, exiting")
        exit(2)

    if args.output:
        outPath = os.path.abspath(args.output)

    if args.priority:
        try:
            priority = int(args.priority)
        except ValueError:
            logging.error("Invalid priority specified: {}".format(args.priority))
            exit(3)

    if args.num_threads:
        try:
            numThreads = int(args.num_threads)
        except ValueError:
            logging.error("Invalid number of threads specified: {}".format(args.num_threads))
            exit(4)

    if args.remove:
        remove = True

    if args.keep_empty:
        keepEmpty = True

    if args.normalize:
        norm = True

def getTalkgroups(file):
    """
    Read talkgroups from the csv file and filter by priority, creating folders for each if they don't exist

    Args:
        csvFile (string): config file to read and parse
    """

    global priority
    global talkgroups

    # Open and parse talkgroups
    with open(file, newline='') as csvFile:
        reader = csv.reader(csvFile, delimiter=',', quotechar='|')
        for row in reader:
            try:
                # Add the talkgroup if its priority is at or above the specified minimum
                if int(row[7]) <= priority:
                    tgId = int(row[0])
                    tgTag = row[3].replace(" ","-")
                    talkgroups.append([ tgId, tgTag ])
                    logging.info("Added talkgroup {} ({})".format(tgId,tgTag))
            except IndexError:
                logging.warn("Improperly formatted csv row, skipping")
                logging.debug(row)

    # Create folders for each TG
    for tgId, tgTag in talkgroups:
        dirName = "{}_{}".format(tgId, tgTag)
        fullDirName = "{}/{}".format(outPath,dirName)
        if not os.path.exists(fullDirName):
            logging.warning("Creating new directory {} for talkgroup".format(dirName))
            logging.debug("Mkdir {}".format(fullDirName))
            os.makedirs(fullDirName)

def combineTalkgroup(tg):
    """
    Main function for combining a talkgroup's recordings

    Args:
        tgId (int): Talkgroup ID (in decimal)
        tgTag (string): Talkgroup tag (for output recording file)
    """

    global recPath
    global outPath
    global remove
    global keepEmpty
    global norm

    tgId, tgTag = tg

    # Collect all the recording files for the specified talkgroup (array will be in [timestamp, path] format)
    recFiles = []
    for root, dirs, files in os.walk(recPath):
        path = root.split(os.sep)
        logging.debug("Searching folder {} for TGID {} ({})".format(root, tgId, tgTag))
        for file in files:
            if file.endswith('.wav') or file.endswith('.m4a'):
                try:
                    fileInfo = file.split('-')
                    if len(fileInfo) < 3:
                        logging.warning("File {} not a valid call recording, skipping".format(file))
                        continue
                    # Get TG as int
                    newtg = int(fileInfo[0])
                    if newtg == tgId:
                        fullPath = "{}/{}".format(root,file)
                        # Get timestamp (unix time) from timestamp/freq block
                        timestamp = datetime.fromtimestamp(int(fileInfo[1].split('_')[0]))
                        # Add entry to file list
                        recFiles.append([timestamp, fullPath])
                        #logging.debug("Found TG file: {}".format(fullPath))
                except ValueError:
                    #logging.debug("Invalid filename: {}, skipping".format(file))
                    pass

    if len(recFiles) == 0:
        logging.info("No files found for TG {} ({}), returning".format(tgId, tgTag))
        return
    else:
        logging.info("Found {} files to process for TG {} ({})".format(len(recFiles), tgId, tgTag))

    # Get oldest file for starting range and get the last 30-minute interval before the current time
    dates = [i[0] for i in recFiles]
    minDate = datetimeFloor(min(dates),timedelta(minutes=30))
    #maxDate = datetimeCeil(max(dates),timedelta(minutes=30))
    maxDate = datetimeFloor(datetime.now(),timedelta(minutes=30))
    if maxDate - minDate > timedelta(days=1):
        minDate = maxDate - timedelta(days=1)
        logging.warning("    Clipped min time to {}, files may remain before this date".format(minDate))
    logging.info("    Time range from {} to {}".format(minDate, maxDate))

    # Generate an array of 30-min spaced datetimes starting with the earliest file rounded down to the nearest 30-minute increment
    timeSegments = list(datetimeRange(minDate, maxDate, timedelta(minutes=30)))
    logging.info("    Processing {} time segment files".format(len(timeSegments)))

    # Iterate through each time segment
    for segment in timeSegments:
        # Generate filename
        #outputFilename = "{}_{}_{}.m4a".format(tgId,tgTag,segment.strftime("%Y%m%d-%H%M%S"))
        outputFilename = "{}_{}_{}.ogg".format(tgId,tgTag,segment.strftime("%Y%m%d-%H%M%S"))
        outputFullpath = "{}/{}_{}/{}".format(outPath, tgId, tgTag, outputFilename)

        outputRec = None    # future file

        filesToDelete = []

        # If file already exists, open it
        if os.path.exists(outputFullpath):
            logging.debug("        File {} already exists, opening and appending any new audio".format(outputFullpath))
            outputRec = AudioSegment.from_file(outputFullpath)
        else:
            # Create a blank file 30 minutes long
            logging.info("        Starting file {}".format(outputFullpath))
            outputRec = AudioSegment.silent(duration=30*60*1000, frame_rate=samplerate)
        
        # Flag for new audio
        hasAudio = False

        # Iterate through each recording
        for file in recFiles:
            # Check if timestamp is inside range
            if segment < file[0] < (segment + timedelta(minutes=30)):
                # Open file
                logging.debug("            opening {}".format(file[1]))
                try:
                    # open based on filetype
                    rec = None
                    if file[1].endswith('m4a'):
                        rec = AudioSegment.from_file(file[1], format='m4a')
                    elif file[1].endswith('wav'):
                        rec = AudioSegment.from_wav(file[1])
                    else:
                        logging.error("Invalid audio file {}, skipping".format(file[1]))
                        continue
                    # Normalize
                    if norm:
                        rec = effects.normalize(rec, headroom=0.25)
                    # Get delta from start of file
                    delta = (file[0] - segment).total_seconds()*1000
                    logging.debug("            Offsetting file {} seconds from start".format(delta))
                    # Add file to output rec at offset
                    #outputRec = outputRec.overlay(rec, position=delta)
                    recBefore = outputRec[:delta]
                    recAfter = outputRec[delta + len(rec):]
                    outputRec = recBefore + rec + recAfter
                    # Flip flag
                    hasAudio = True
                    # Remove if enabled
                    if remove:
                        logging.debug("            marking file {} for deletion".format(file[1]))
                        filesToDelete.append(file[1])
                except pydubex.CouldntDecodeError:
                    logging.error("Got error decoding file {}, skipping and not removing".format(file[1]))
                    continue 

        # Trim to 1-second silence if it's an empty file or skip if we aren't keeping empty files
        if not hasAudio:
            if keepEmpty:
                outputRec = outputRec[0:1000]
                logging.warning("        No audio for time range, shortening to 1s of silence")
            else:
                logging.warning("        No audio for time range, not saving")
                continue
        
        # Apply post-processing to audio
        #logging.debug("        Applying effects to output clip")
        #outputRec = effects.compress_dynamic_range(outputRec)
        #outputRec = effects.normalize(outputRec, headroom=1.0)

        # Save
        logging.info("        Saving file {}".format(outputFullpath))
        outputFile = outputRec.export(outputFullpath, 
                        format="opus",
                        bitrate="18k",
                        parameters=["-application", "voip", "-cutoff", "6000"])

        # Make sure the output file was written before deleting
        if os.path.exists(outputFile.name) and remove:
            logging.info("File {} successfully written, deleting source files".format(outputFile.name))
            for file in filesToDelete:
                logging.debug("        removing file {}".format(file))
                os.remove(file)


def main():
    """
    Main runtime
    """

    global trunkCsv
    global recPath
    global outPath
    global numThreads

    parseArgs()

    logging.info("Using trunking CSV: {}".format(trunkCsv))
    logging.info("Using trunk-recorder archive path: {}".format(recPath))
    logging.info("Using output folder: {}".format(outPath))
    logging.info("Using threads: {}".format(numThreads))

    getTalkgroups(trunkCsv)

    logging.info("Total talkgroups to process: {}".format(len(talkgroups)))

    # Single-threaded version
    #for tg in talkgroups:
    #    combineTalkgroup(tg)

    # Multithreaded Version
    p = Pool(processes=numThreads)
    result = p.map(combineTalkgroup, talkgroups)

    logging.info("Finished!")

if __name__ == "__main__":
    main()