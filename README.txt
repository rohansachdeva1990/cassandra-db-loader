DATABASE LOADER                                                    Revision : 02
--------------------------------------------------------------------------------
WHAT THIS PACKAGE DOES

  These scripts are used to populate the required database with the values ranges
  as defined in the appropriate configuration files. As of now, this package is 
  responsible for populating Cassandra Database.
  
--------------------------------------------------------------------------------
CHANGES IN THIS RELEASE
  Version  1.0.0.2
  Date     09/28/2016
  Author   Rohan Sachdeva

[Prerequisites]
- Python 2.7
- Python Cassandra Driver
- Cassandra DB
  
[Important updates]
- Improved Performance.

[New functions or enhancements]
- Configure no. of session pools.
- Cassandra statistics.
- Delete capability.

[Problem fixes]
- NA

[TODO]
- Add support for multi-processing logging.

--------------------------------------------------------------------------------

INSTALLATION REQUIRED

	1. "pip" is the suggested tool for installing packages. It will handle installing all Python 
	    dependencies for the driver at the same time as the driver itself.
	         
        2.  To install the driver*:
    	 	pip install cassandra-driver

INSTRUCTIONS

	1. Update cluster configuration in config/config.ini
	2. Modify the value ranges as per requirement in config/subscriber_data.xml
	3. To run :-
		
		    For Linux/Unix
		    	- sh run.sh
		    
		    For Windows
		    	- run.bat
