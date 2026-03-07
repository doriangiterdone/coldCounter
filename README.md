# coldCounter

coldCounter is an open source data infrastructure project designed to aggregate, normalize, and document publicly available information related to immigration enforcement outcomes in the United States.

The system was created on March 4, 2025 by No Concentration Camps in Colorado to support legal researchers, journalists, policy analysts, and civil rights organizations seeking transparent and reproducible data on immigration enforcement practices.

coldCounter provides a portable SQLite data mart along with reproducible Python scripts used to construct the database and generate analytical reports.

# Purpose

Immigration enforcement data in the United States is distributed across multiple federal agencies and public publications. These sources often differ in formatting, classification systems, and accessibility.

coldCounter was developed to aggregate immigration enforcement data derived from publicly available government sources, normalize offense classification codes used in enforcement systems, provide a transparent and reproducible database structure, enable legal researchers to evaluate enforcement classifications, and support litigation, policy analysis, and investigative journalism.

The project emphasizes traceability, reproducibility, and documentation so that independent researchers can understand how enforcement classifications are represented within the database.

# System Overview

coldCounter consists of two primary functional layers: database construction and analytical reporting.

# Database Construction

The database is built using Python scripts that generate and populate a SQLite data mart.

SQLite was selected because it allows the entire dataset to be distributed as a portable single file database which can easily be shared among researchers, opened in database tools, or attached to analytical workflows.

The construction scripts perform tasks including creating the database schema, importing immigration enforcement data, normalizing classification systems, and populating dimensional reference tables.

# Analytical Reporting

The project includes scripts used to generate aggregated reports from the database.

These scripts allow users to produce summary outputs useful for legal analysis, policy research, investigative reporting, and statistical summaries of enforcement data.

# Repository Structure

coldCounter/

setup/
Python scripts used to generate and populate the database

reports/
Python scripts used to aggregate data from the database

README.md

# Building the Database

The coldCounter database can be built locally using the provided setup scripts.

##Step 1

Run the primary ETL process:

python setup/etl_DeportationDataProject_to_coldCounter.py

This script performs the primary Extract Transform Load process that constructs the base database and imports immigration enforcement data.

##Step 2

Populate the offense code dimension:

python setup/dim_ncic_offense_codes.py

This script populates the dimensional table containing NCIC offense code references which are used to standardize offense classifications within the database.

After both scripts have been executed successfully the coldCounter SQLite database will be fully constructed and ready for analysis.

# Data Sources

coldCounter relies exclusively on publicly available government and institutional publications related to immigration enforcement and criminal offense classification.

Reference materials include publications from:

U.S. Immigration and Customs Enforcement

U.S. Department of Homeland Security

Federal Bureau of Investigation

United States Sentencing Commission

U.S. Department of Justice

These materials are used to construct normalized classification references within the database.

# Data Provenance and Methodology

coldCounter is designed to ensure that all database content is traceable to publicly available source materials. The project does not rely on proprietary datasets or unpublished government records.

# Source Material

All reference data incorporated into the database originates from publicly published materials issued by United States government agencies and related institutions. These materials provide the legal and administrative definitions used to construct dimensional reference tables within the database.

# Data Transformation Process

coldCounter is built through a reproducible Extract Transform Load workflow implemented in Python.

The construction process follows three stages.

# Extraction

Publicly available datasets and classification references are collected from government publications and documentation.

# Transformation

Extracted data is normalized into consistent formats suitable for database storage. This process includes standardizing classification codes, harmonizing naming conventions, and structuring reference data into dimensional tables.

# Loading

The normalized data is loaded into a SQLite data mart designed for analytical queries and reporting.

The ETL process is executed through the repository setup scripts allowing the database to be rebuilt deterministically by independent researchers.

# Reproducibility

Because the database is generated through scripted processes users can independently reconstruct the dataset by executing the provided setup scripts.

This reproducibility allows researchers to verify database schema design, data transformation methods, and classification mappings.

The goal is to ensure that the coldCounter database functions as a transparent analytical tool rather than a closed dataset.

# Scope and Limitations

coldCounter aggregates and normalizes publicly available reference materials related to immigration enforcement classification systems.

The project does not independently verify the accuracy of the underlying government publications from which reference data is derived.

coldCounter is intended to document classification structures and provide analytical infrastructure for examining enforcement related datasets. The database should not be interpreted as an authoritative statement of federal enforcement policy or as a comprehensive record of immigration enforcement activity.

Users of the database should consult original government publications when making legal or policy determinations.

# Intended Use

coldCounter is designed to support legal research, policy analysis, investigative journalism, academic research, and civil rights monitoring.

The system provides a structured representation of immigration enforcement classifications that can be examined independently by researchers and legal professionals.

# Citation

Researchers, journalists, and legal practitioners who reference coldCounter in publications or filings are encouraged to cite the system using the following format.

Recommended Citation

No Concentration Camps in Colorado. coldCounter: Immigration Enforcement Data Infrastructure. Version 1.0. Created March 4, 2025.

Example Academic Citation

No Concentration Camps in Colorado, coldCounter: Immigration Enforcement Data Infrastructure (Mar. 4, 2025).

Example Legal Citation (Bluebook Style)

coldCounter: Immigration Enforcement Data Infrastructure, No Concentration Camps in Colorado (Mar. 4, 2025).

When possible citations should also include the repository URL where the database and documentation are hosted.

# License

This project is released as open source software under the MIT License.

Copyright 2025 No Concentration Camps in Colorado

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction. This includes without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions.

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT, OR OTHERWISE, ARISING FROM, OUT OF, OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
