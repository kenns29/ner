# NER Extractor

The purpose of the program is to extract named entities from tweet text, and geocode the tweets based on the extracted entities. This program is capable of quickly processing historical tweets extracted by the Tweettracker program, as well as updating newly extracted tweets in real time. The program ensures all potential errors are handled properly.

## library used

* Stanford core-nlp library is used for the named entity recognition
* Geoname service is used for geocoding

## How to Compile

To compile the code, use gradle:

gradle build

## How to run

To run the code, make sure to put the config.properties file in the same directory as the runnable file. Then do ./ner.

## Keep track of the progress

* While the program is running, the current progress can be shown in a http server, with host and port defined in the config.properties file.
* **[host]:[port]/status** - shows the current status of the program
* **[host]:[port]/progress** - shows the current status of the program
* **[host]:[port]/time** - shows the detailed run time cost of each component in the program.
* **[host]:[port]/jsonsafestid** - shows the current safest id in the json format
* **[host]:[port]/jsonstatus** - shows the current status in json format

* check the log files in the /log folder

## Documentation

for furthur information please check the doc/ directory.
