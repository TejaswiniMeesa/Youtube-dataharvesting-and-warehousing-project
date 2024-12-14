## Introduction:
 The project aims at developing a Streamlit app for harvesting and warehousing the data from Youtube by collecting, storing and analyzing the channel information, video information and comment information.
## Skills take away:
 Python Scripting,
 Data collection,
 Streamlit,
 API integration,
 Data management using SQL
 ## Libraries used:
  + googleapiclient.discovery
  + Streamlit
  + sql.connector
  + Pandas
## Data collection:
  For retrieving information from different channels in Youtube we use an API key that is generated using the youtube data API. By calling this API we retrieve data in three categories like the channel 
 information, video information and the comments information using the corresponding channel ID. For this purpose, a connection to the YouTube API V3 is established by utilizing the Google API client library for 
 Python.
## Data storing:
 The retrieved information is stored in the SQL database by creating tables and inserting (channels, video, comment) information in corresponding tables, a connection to SQL server is established by utilizing 
 sql.connector library for Python.
 ## Data analysis:
  By using the Youtube data inserted in the tables some SQL queries are answered.
## Usage guide for streamlit app:
   ### 1. Data Harvesting:
  * Select "Data Harvesting" from the sidebar.
  * Enter the channel ID in the provided input box.
  * Click on "Fetch Channel Data" to retrieve channel information (similarly for video and comment data).       
  ### 2. Data Warehousing:
  * Select "Data Warehousing" from the sidebar.
  * Enter the channel ID in the provided input box.
  * Click on "store data" then the retrieved information is stored in the MySQL database.   
  ### 3. Data Analysis:
  * Select "Data Analysis" from the sidebar.
  * Choose a pre-defined SQL query from the available list. 
  * Click on "Execute" to display the results of the query.  
 
  
  
