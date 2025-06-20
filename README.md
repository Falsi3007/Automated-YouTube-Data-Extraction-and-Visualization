# 🎥 Automated YouTube Data Extraction and Visualization

## 📌 Overview
A real-time data pipeline that leverages the YouTube Data API to automate data extraction, transformation, and visualization. The project enables structured analysis of performance metrics such as engagement trends, content effectiveness, and growth patterns using Python, Snowflake, and Power BI.

## 🔄 Project Workflow
YouTube Data API  
    ↓  
Python ETL Scripts  
    ↓  
Cleaned & Transformed Data  
    ↓  
Snowflake Data Warehouse  
    ↓  
Power BI Dashboards  


## 🛠️ Tech Stack

| Tool/Tech        | Role                                                        |
|------------------|-------------------------------------------------------------|
| Python           | API integration and data extraction                         |
| SQL              | For transformations and data modeling                       |
| Snowflake        | Cloud data warehouse for storing and querying data          |
| Power BI         | Dashboard for insights and KPIs                             |
| Git/GitHub       | Version control and collaboration                           |
| Pandas           | Data Manipulation                                           |
| Jupyter Notebook | Interactive Data Analysis                                   |

## 📁 Folder Structure

```
youtube-data-pipeline/
├── api_scripts/
│   ├── channelConn.py
│   ├── playlistConn.py
│   └── videoConn.py
├── snowflake_scripts/
│   ├── snowflake.sql
├── dashboard/
│   └── YouTube_Analytics_Report.pbix
├── README.md
└── .env (for credentials)
```

## 📊 Power BI Dashboard
Access: YouTube_Analytics_Report.pbix

Modules:
Overview Summary
channel
playlist
videos

## 🧠 Future Enhancements
- Schedule pipeline automation using Apache Airflow
- Integrate NLP for comment sentiment analysis
- Add predictive analytics for performance forecasting
