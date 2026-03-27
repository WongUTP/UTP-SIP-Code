# Semiconductor Manufacturing ETL Pipeline

## Overview
This repository contains the backend architecture for an automated Extract, Transform, and Load (ETL) pipeline developed for a semiconductor manufacturing environment. The system automates the extraction of legacy defect logs and high-volume machine parameters, replacing manual data entry with a high-speed, relational database architecture.

## Technologies Used
* **Python (Pandas):** For high-speed data extraction, wide-to-long data melting, and relational merging.
* **Visual Basic for Applications (VBA):** For automated Outlook email parsing and executing background scripts.
* **Excel Power Query:** For final frontend data modeling and dashboard integration.

## Key Features
* **Automated Data Ingestion:** VBA scripts autonomously scrape targeted engineering emails to extract daily defect logs, bypassing embedded images to save processing power.
* **Intelligent File Caching:** Python scripts utilize MD5 cryptographic hashing to track processed files, bypassing redundant data and reducing computational latency by over 90%.
* **Dynamic Time-Window Merging:** Replaces static defect creation dates with dynamic factory "Track-Out Times" (TCKO) to accurately correlate physical molding defects with specific machine parameters (e.g., Clamp Force, Transfer Pressure) within a strict one-hour window.

## Impact
In a production environment, this architecture reduced a 30-minute manual data merging process down to under 5 minutes (an 84% reduction in processing time). The automated pipeline autonomously manages the ingestion of thousands of data points, saving an estimated 50 hours of manual engineering labor annually.

*(Note: Sensitive company data, server IPs, and proprietary lot numbers have been scrubbed from this repository for security purposes).*
