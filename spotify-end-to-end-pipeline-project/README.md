# 🎵 Spotify Data Pipeline: End-To-End Python Data Engineering Project 🎧

## Overview

Welcome to the Spotify Data Pipeline project! This project demonstrates an end-to-end data engineering pipeline,
integrating with the Spotify API to extract, transform, and analyze data, all within a cloud-native environment.

From data extraction to automated processing and analytics, this repository showcases how to build a robust data pipeline using 
Python and AWS services. Whether you're a data engineer, a music enthusiast, or someone curious about cloud technologies,
this project offers valuable insights into building scalable and efficient data pipelines.

### Features

- **Integration with Spotify API:** Seamlessly connect with Spotify's API to extract relevant music data.

- **Automated Data Extraction on AWS Lambda:** Deploy Python scripts to AWS Lambda to automate data extraction, ensuring efficient use of resources and minimal maintenance.

- **Trigger-based Automation:** Implement triggers to run the extraction process at regular intervals, making the pipeline fully automated.

- **Data Transformation:** Write custom transformation functions to clean, normalize, and prepare the data for analysis.

- **Automated Transformation Triggers:** Set up triggers to automatically initiate data transformation processes, ensuring data is always ready for consumption.

- **Data Storage on S3:** Store extracted and transformed data files securely on AWS S3, organized for easy access and retrieval.

- **Analytics with Glue and Athena:** Build powerful analytics tables using AWS Glue and Athena, enabling quick queries and insights from the stored data.

### Project Structure

```
.
├── src/
│   ├── code.ipynb                               # Code, which is used in local system             
│   │── spotify_api_data_extract.py              # AWS lambda function for extracting data from Spotify API
│   │── spotify_transformation_load_function.py  # AWS lambda transformation functions for data cleaning and processing
└── README.md                                    # Project documentation
```

### Contributing
I welcome contributions to enhance this project! Feel free to fork the repository, create a new branch, and submit a pull request.

### Contact
For any questions or support, please open an issue on this repository or reach out via email at kenilsutariya030@gmail.com
