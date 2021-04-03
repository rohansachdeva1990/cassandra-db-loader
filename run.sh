#!/bin/sh

cd bin/.

# Getting my user Id ...
MY_USER_ID=`id |cut -d '=' -f2|cut -d '(' -f1`

# Getting my process id as per my user id ...
MY_PROCESS_ID=$(ps -eao user,uid,pid,ppid,comm,args | grep python | grep main | awk '{ if($2=="'"$MY_USER_ID"'") {print $"3";} }')


# Check if database loader is already running and give a warning message
status=0;
if [ $MY_PROCESS_ID ]
   then
     echo "Database Loader is running.. ( PUID: " $MY_PROCESS_ID ")";
else
    status=1;
fi

if [ $status -ne 1 ]
then
	echo "Database Loader is running please kill it before restarting";
    exit 1
fi

# finally start the process
python main.py $1 $2 &