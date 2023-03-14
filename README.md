# ETL Python Script in semi-structured data
An ETL Python Script that performs the ETL process of JSON Data, transforms it to DataFrame, then loads it into CSV files.

## Project Description:
A script that transforms the JSON files to a DataFrame and commits each file to a separate CSV file in the target directory.

The Script does the following:
- Reads JSON file from a directory using positional specific argument.
- Extracts the data, cleans, and transforms it.
- Checks if the files have any duplicates and remove them.
- Uses the optional argument "-u" to maintain the UNIX format for the timestamp. 
- Prints a message after converting each file with the number of rows transformed and the path of the file.
- Creates CSV files that contain the final output in a CSV format.
- Prints the total execution time.

## Problem Description:

In 2012, URL shortening service Bitly partnered with the US government website USA.gov to provide a feed of anonymous data gathered from users who shorten links ending with .gov or .mil.

The text file comes in JSON format and with some keys and their description. 

- ```a``` Denotes information about the web browser and operating system.
- ```tz``` time zone.
- ```r``` URL the user come from.
- ```u``` URL where the user headed to.
- ```t``` Timestamp when the user start using the website in UNIX format.
- ```hc``` Timestamp when user exit the website in UNIX format.
- ```cy``` City from which the request intiated.
- ```ll``` Longitude and Latitude.

The output CSV files have the following columns:

- ```web_browser``` The web browser that has requested the service.
- ```operating_sys``` operating system that intiated this request.
- ```from_url``` The main URL the user came from in a short format.
- ```to_url``` The main URL the user went to in a short format.  
- ```city``` The city from which the the request was sent.
- ```longitude``` The longitude where the request was sent.
- ```latitude``` The latitude where the request was sent.
- ```time_zone``` The time zone that the city follow.
- ```time_in``` Time when the request started.
- ```time_out``` Time when the request is ended.

## Tools and Technologies:
- Python 
- Pandas
- NumPy
- Jupyter Notebook
- JSON
