# Tempus Data Engineering Challenge



## Objective of the Project

Create a pipeline to retrieve the top headlines of the day from english sources from the News API and store them by source in an S3 bucket.  As a bonus another pipeline was created to look for a specific set of key words within the top headlines of the day and store them by key word in another S3 bucket.

## Tempus Challenge DAG
**The Tempus challenge DAG uses the following tasks:**
> ### getheadlines
Make a call to the News API filtered for sources that use the english language. Return the JSON as a python dictionary.
> ### tabularize
Pull the JSON returned by the previous task using the xcom method. Flatten the JSON (python dictionary) into a dictionary keyed by the news source where the values are a list of headlines each represented by a dictionary containing the data for each headline return the by the api. Write each lists to CSV file in a temporary directory to be uploaded to S3 in the next task. Each file is named for the source responsible for the headline. Return the name of the directory the CSVs are stored in.
>###  upload
Pull the name of temporary CSV directory and and upload the files to S3 using boto3. use the csv direcotry name to determine which tabularize function came prior and use this to decide which bucket to upload too.   The files are placed in S3 directories according to their source and keyed by the date they were retrieved.  Clean up by deleting the temporary CSV directory and all the files within it.


## Keyword DAG
**The Keyword DAG uses the following tasks:**
> ### getheadlines
Make a call the the News API for each word in a list of keywords set in the values module.  If an exception is raised for a keyword, log it and continue.  if an exception is raised for all of the key words log it and re raise to trigger a retry and then a failure upon a second occurrence of the above. return the JSON retrieved as a python dictionary.
> ### tabularize
Pull the JSON returned by the previous task using the xcom method. Flatten the JSON (python dictionary) into a dictionary keyed by the keyword where the values are a list of headlines, each represented by a dictionary containing the data for each headline return the by the api. Write each list to CSV file in a temporary directory to be uploaded to S3 in the next task. Each CSV file is named for the keyword that resulted that it is related to. Return the name of the directory the CSVs are stored in.
>###  upload
Reuse the upload function from the Tempus Challenge DAG.
