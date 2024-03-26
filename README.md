# 💊 Pharmazer-Pipeline
An on-demand pipeline that uses machine learning and natural language processing (NLP) to help connect matching records for a fictional pharmaceutical company (Pharmazer).

## 📝 About
PharmaZer is a fictional pharmaceutical company that is currently focused on improving our understanding of Sjogren Syndrome. PharmaZer is interested in collating research papers from various research institutions to achieve this goal.

One of the challenges that PharmaZer may face when collating research papers from various sources is the issue of inconsistent naming of organisations. This can occur when the same organisation is referred to by different names in different papers, making it difficult to accurately match and collate the data. This inconsistency in naming can also have an impact on the results of data analysis, as it can lead to errors in data aggregation and hinder the ability to accurately compare the findings of different studies.

The client has given us a dataset retrieved from PubMed containing search results for the term sjogren syndrome. We also have access to the Global Research Identifier Database (GRID) datasets which contains the standard names and alias of multiple medical institutions. We can use the standardised GRID dataset to identify all the multiple occurrences of certain institutions with the PubMed Articles correctly.

The main goal of this project is to navigate the PubMed XML file, extracting only relevant fields, enriching and cleaning them using regular expressions and other tools, and finally performing data matching in the affiliations, returning a single csv file. On top of this, it serves as a chance to learn about machine learning and natural language processing (NLP). 

## Setup ⚙️
- Install requirements using `pip3 install -r requirements.txt`
- Download the spacy language model by running `python -m spacy download en_core_web_sm`
- Install boto3 type hints by running `python -m pip install 'boto3-stubs[essential]'`
- Create a `.env` file with the following information:
  - `AWS_ACCESS_KEY_ID=XXXXX`
  - `AWS_SECRET_ACCESS_KEY=XXXXX`
  - `INPUT_BUCKET=XXXXX`
  - `OUTPUT_BUCKET=XXXXX`
- Create a folder named `grid_data` - which should contain two files named [`addresses.csv`](https://sigma-resources-public.s3.eu-west-2.amazonaws.com/pharmazer/addresses.csv) and [`institutes.csv`](https://sigma-resources-public.s3.eu-west-2.amazonaws.com/pharmazer/institutes.csv)

## Files explained
- The `comp_data.py` file contains functions which create datasets to be used in the pipeline
- The `extract.py` file contains functions used for extracting the data from the XML file in the S3 bucket
- The `transform.py` file contains functions used for transforming the data into the required format
- The `pipeline.py` file brings all these functions together and runs the pipeline from start to finish

## Running the Pipeline
- To run the pipeline, simply use the command `python3 pipeline.py`

## Outputs
- Upon completion of the pipeline, a CSV file containing all the relevant data will be uploaded to an S3 bucket on AWS

## Deployment Strategy
- I chose to go with AWS Fargate since I didn't need to change any of my code. 
- Using AWS Lambda was also an option, however since my script was taking longer than 15 mins to run when I wanted to deploy it to the cloud, I would have had to split up my code into smaller chunks which could then be run as separate lambda functions.
- This would have added a lot more complexity to the system, for example I would have needed to transfer the data from one lambda function to the next, and it would have involved rewriting a lot of my code just to fit it into the Lambda time limit. 