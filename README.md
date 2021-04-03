# Cassandra Database Populator

These scripts are used to populate the required database with the values ranges as defined in the appropriate configuration files. As of now, this package is responsible for populating Cassandra Database.

## Prerequisites

- Python 2.7
- Python Cassandra Driver
- Cassandra DB

## Installation

- "pip" is the suggested tool for installing packages. It will handle installing all Python dependencies for the driver at the same time as the driver itself.

- To install the driver:
  - pip install cassandra-driver

## INSTRUCTIONS

Update cluster configuration in config/config.ini 2. Modify the value ranges as per requirement in config/subscriber_data.xml 3. To run :-

    - For Linux/Unix
    	- sh run.sh

    - For Windows
    	- run.bat
