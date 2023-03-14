#!/usr/bin/env python
# coding: utf-8

# ### Problem Descripition 
# 
# In 2012, URL shortening service Bitly partnered with the US government website USA.gov to provide a feed of anonymous data gathered from users who shorten links ending with .gov or .mil.
# 
# The text file comes in JSON format and here are some keys and their description. They are only the most important ones for this task.

# |key| description |
# |---|-----------|
# | a|Denotes information about the web browser and operating system|
# | tz | time zone |
# | r | URL the user come from |
# | u | URL where the user headed to |
# | t | Timestamp when the user start using the website in UNIX format |
# | hc | Timestamp when user exit the website in UNIX format |
# | cy | City from which the request intiated |
# | ll | Longitude and Latitude |

# In the cell, I tried to provide some helper code for better understanding and clearer vision
# 
# -**HINT**- Those lines of code may be not helping at all with your task.

# In[21]:


# I will try to retrieve one instance of the file in a list of dictionaries
#import json
#records = [json.loads(line) for line in open('usa.gov_click_data_1.json')]
# Print the first occurance
#records[0]


# ## Required

# Write a script can transform the JSON files to a DataFrame and commit each file to a sparete CSV file in the target directory and consider the following:
# 
#         

# All CSV files must have the following columns
# - web_browser
#         The web browser that has requested the service
# - operating_sys
#         operating system that intiated this request
# - from_url
# 
#         The main URL the user came from
# 
#     **note**:
# 
#     If the retrived URL was in a long format `http://www.facebook.com/l/7AQEFzjSi/1.usa.gov/wfLQtf`
# 
#      make it appear in the file in a short format like this `www.facebook.com`
#      
#     
# - to_url
# 
#        The same applied like `to_url`
#    
# - city
# 
#         The city from which the the request was sent
#     
# - longitude
# 
#         The longitude where the request was sent
# - latitude
# 
#         The latitude where the request was sent
# 
# - time_zone
#         
#         The time zone that the city follow
#         
# - time_in
# 
#         Time when the request started
# - time_out
#         
#         Time when the request is ended
#         
#         
# **NOTE** :
# 
# Because that some instances of the file are incomplete, you may encouter some NaN values in your transforamtion. Make sure that the final dataframes have no NaNs at all.

# # Let's Go..

# ### Step 1: Load the Data

# In[22]:


# Import the required libraries:

import pandas as pd
import numpy as np
import os
import re
import json
import warnings
import argparse
import time
from urllib.parse import urlparse
from datetime import datetime as dt


# In[105]:


print("Enter arguments:"+"\n"+"-i input file path"+"\n"+"-o output file path")


# In[23]:


# Handling the Arguments
parser = argparse.ArgumentParser (description="Dina Hosny Python")
parser.add_argument('-i','--inputPath',help="The path for the input file",type=str)
parser.add_argument('-o','--outputPath',help="The path for the output file",type=str)
parser.add_argument('-u','--unix',help="if passed the time will be kept in UNIX format", action="store_true")
args, unknown = parser.parse_known_args()


# In[24]:


# Record the Start Time:
start = time.time()


# In[25]:


# Take the JSON file path from the user and save it into variable:
##file_path = input('Enter a file path: ')
file_path = args.inputPath


# In[29]:


# Print number of unique and duplicated records:
records = [json.loads(line) for line in open(file_path)]

Unique_records =[]
records_num = 1

for record in records:
    records_num +=1
    if record not in Unique_records:
        Unique_records.append(record)

print ("File contains "+ str(records_num)+ " record"+ "\n")        
if len(Unique_records)<len(records):
    print("There Are: " + str(len(records)-len(Unique_records)) + " duplicate values in the file")
else: print ("No Duplicate Values in the file")


# In[30]:


# Load the data into a datafram:
# orient records and lines true cause records seperated by '\n'
first_df = pd.read_json(file_path, orient='records', lines=True)


# In[12]:


# Check the dataframe:
first_df


# ### Step 2: Exploring Data:

# In[31]:


# Explore 'a' column that contains the browser and operating system type: 
first_df['a']


# In[32]:


# Explore 'r' column that contains the from url:
first_df['r']


# In[33]:


# Explore 'u' column that contains the to url:
first_df['u']


# In[34]:


# Explore 'cy' column that contains the city info:
first_df['cy']


# In[35]:


# Explore 'll' column that contains the longitude and latitude info in a list:
first_df['ll']


# In[36]:


# Explore 'tz' column that contains the time zone info:
first_df['tz']


# In[37]:


# Explore 't' column that contains the timestamp in info:
first_df['t']


# In[38]:


# Explore 'hc' column that contains the timestamp out info:
first_df['hc']


# ### Step 3: Working with data

# In[51]:


# Create the output dataframe that contains the required data:
bitly_data = pd.DataFrame()


# In[52]:


# Check the creation:
bitly_data 


# #### 1- Extract the Web Browser type:

# In[53]:


# Create web browser column in the output dataframe that contains the web browser type
# Extract the web browser type from the column 'a'
# Web browser type is the first word in the column and ends in '/' 
bitly_data['web_browser'] = first_df['a'].str.split('/').str[0]


# In[54]:


# Check the data:
bitly_data['web_browser']


# In[55]:


# Mozilla and Opera are web browsers while GoogleMaps not! 
# Handling External programs that are not web browsers
bitly_data['web_browser'] = bitly_data['web_browser'].apply(lambda x: x if x in ['Mozilla', 'Opera'] else 'External Program' )


# In[56]:


# Check the data:
bitly_data['web_browser']


# #### 2- Extract the Operating System type:

# In[57]:


# Create operating_sys column in the output dataframe that contains the operating system type
# Extract the operating system type from the column 'a'
# Operating system type is founded between '()' 
bitly_data['operating_sys'] = first_df['a'].str.split('(').str[1].str.split(')').str[0]


# In[58]:


# Check the data:
bitly_data['operating_sys']

# Extracted operating system type contains unuseful data, so it'll be ommited  


# In[59]:


# Ommit the unuseful data from the operating system column
# The useful data which is the operarting system name and version is the first part and ends with ';'
bitly_data['operating_sys'] = bitly_data['operating_sys'].str.split(';').str[0]


# In[60]:


# Check the data:
bitly_data['operating_sys']

# Looks better :)


# In[61]:


# Fill the NaN values with 'unknown'
bitly_data['operating_sys'] = bitly_data['operating_sys'].fillna('Unknown')


# In[62]:


# Chwck the final result:
bitly_data['operating_sys']


# #### 3- Extract From URL:

# In[63]:


# Create from_url column in the output dataframe that contains the URL that user came from
# Extract the from URL from the column 'r'
# From URL is too long, so it should be in the short format that is before third '/'

bitly_data['from_url'] = first_df['r'].str.split('/').str[2]


# In[64]:


# Check the data:
bitly_data['from_url']


# In[65]:


# Fill the NaN values with 'Direct'
bitly_data['from_url'] = bitly_data['from_url'].fillna('Direct')


# In[66]:


# Check the final result:
bitly_data['from_url']


# #### 4- Extract To URL:

# In[67]:


# Create to_url column in the output dataframe that contains the URL that user went to
# Extract the to URL from the column 'u'
# From URL is too long, so it should be in the short format that is before third '/'

bitly_data['to_url'] = first_df['u'].str.split('/').str[2]


# In[68]:


# Check the data:
bitly_data['to_url']


# #### 5- Extract City

# In[69]:


# Create city column in the output dataframe that contains the city that request was sent from
# Extract the city from the column 'cy'

bitly_data['city'] = first_df['cy']


# In[70]:


# Check the data:
bitly_data['city']


# In[71]:


# Fill the Nan values with 'Unknown':
bitly_data['city'] = bitly_data['city'].fillna('Unknown')


# In[72]:


# Chwvk the final result:
bitly_data['city']


# #### 6- Extract Longitude:

# In[73]:


# Create longitude column in the output dataframe that contains longitude info
# Extract the longitude from the column 'll'
# The column contains list, the first item is the longitude

bitly_data['longitude'] = first_df['ll'].str[0]


# In[74]:


# Check the data:
bitly_data['longitude']


# In[75]:


# Fill the Null data with 'Not Detected'
bitly_data['longitude'] = bitly_data['longitude'].fillna('Not Detected')


# In[76]:


# Check the result:
bitly_data['longitude']


# #### 7- Extract the latitude:

# In[77]:


# Create latitude column in the output dataframe that contains latitude info
# Extract the latitude from the column 'll'
# The column contains list, the second item is the longitude

bitly_data['latitude'] = first_df['ll'].str[1]


# In[78]:


# Check the data:
bitly_data['latitude']


# In[79]:


# Fill the Null data with 'Not Detected'
bitly_data['latitude'] = bitly_data['latitude'].fillna('Not Detected')


# In[80]:


# Check the data:
bitly_data['latitude']


# #### 8- Extract the Time Zone:

# In[81]:


# Create time_zone column in the output dataframe that contains the time zone that city follows
# Extract the the time zone from the column 'tz'

bitly_data['time_zone'] = first_df['tz']


# In[82]:


# Check the data:
bitly_data['time_zone']


# In[106]:


# There're null values but stored as blank space:
# Replace the empty values with 'Unknown'
bitly_data['time_zone'] = bitly_data['time_zone'].replace('', 'Unknown')


# #### 9- Extract Time In:

# In[83]:


# Create time_in column in the output dataframe that contains when request started
# Extract the time in from the column 't'

bitly_data['time_in'] = first_df['t']


# In[98]:


# Check the data:
bitly_data['time_in']


# In[84]:


# Keep unix timestamp if argument -u passed:
if args.unix:
    bitly_data['time_in'] = first_df['t']

# If not passed, Convert the data into timestamp form using fromtimestamp():
else:
    bitly_data['time_in'] = bitly_data['time_in'].apply(lambda x : dt.fromtimestamp(x))


# In[85]:


# Check the final result:
bitly_data['time_in']


# #### 10- Extract Time Out

# In[86]:


# Create time_out column in the output dataframe that contains when request ended
# Extract the time in from the column 'hc'

bitly_data['time_out'] = first_df['hc']


# In[87]:


# Check the data:
bitly_data['time_out']


# In[88]:


# Keep unix timestamp if argument -u passed:
if args.unix:
    bitly_data['time_out'] = first_df['hc']

# If not passed, Convert the data into timestamp form using fromtimestamp():
else:
    bitly_data['time_out'] = bitly_data['time_out'].apply(lambda x : dt.fromtimestamp(x))


# In[89]:


# Check the final result:
bitly_data['time_out']


# ### Step 4: Export Final Data to CSV file:

# In[90]:


# Check the final dataframe:
bitly_data


# In[91]:


# Check the final dataframe shape:
bitly_data.shape


# In[100]:


# Take the final CSV file path from the user and save it into variable:
output_name= input('Enter the output file name: ')
###output_path = input('Enter the path to save CSV in: ')


# In[101]:


# Load the data into csv file:
###bitly_data.to_csv(output_path+'\\'+ output_name+ '.csv')
bitly_data.to_csv(args.outputPath+'\\'+ output_name+ '.csv')


# In[ ]:


# Print the number of rows transformed:
print ("The File in the path: " , args.inputPath , " was succefully transformed")

print ("There was " + str(len(bitly_data.index)) + " rows transformed" )


# In[102]:


# Record the end time:
end = time.time()


# In[103]:


# Print the execution time:
# difference between start and end time in milli. secs
print("The time of execution of this script is :",
      (end-start) * 10**3, "ms")


# In[ ]:


# End .. :)

