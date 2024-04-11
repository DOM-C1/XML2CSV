# XML2CSV for PharmaZer

## Project Overview
XML2CSVPlus is a cloud-based solution designed for PharmaZer to facilitate the process of matching institutions between a given Pubmed XML file and the GRID dataset, with the output being a single .csv file. This project is part of a broader initiative to harness the power of cloud computing for accelerating medical research by providing a scalable and automated system for data transformation and analysis.

### Features
- Automated listening for new XML files in an AWS S3 bucket (`sigma-pharmazer-input`).
- Triggered execution of the pipeline upon detecting a new file.
- Automated output of the resulting CSV file to another S3 bucket (`sigma-pharmazer-output`).
- Notifications to users at the start and end of each task.

## Requirements
To use this solution, ensure you have access to AWS services, including S3 buckets and EventBridge. The input bucket is already configured to send event notifications, facilitating the automated pipeline's trigger mechanism.

## Getting Started

### Setting up the Infrastructure
1. Ensure you have two S3 buckets: `sigma-pharmazer-input` for input XML files and `sigma-pharmazer-output` for the resulting CSV files.
2. Configure AWS EventBridge to listen for `Put` events from the `sigma-pharmazer-input` bucket.

### Automating Deployment
1. Use the provided Python script to automate the deployment and execution of the pipeline. The script listens for new files in the `sigma-pharmazer-input` bucket, processes them, and outputs CSV files to the `sigma-pharmazer-output` bucket.
