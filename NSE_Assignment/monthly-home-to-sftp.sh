#!/bin/bash


source /home/rely/.bashrc
TEMP_PATH="/home/rely/tmp_data/equifax_homes"
mkdir $TEMP_PATH
/home/rely/hadoop/bin/hdfs dfs -copyToLocal /datascience/geo/NSEHomes/monthly/equifax/to_push/$fecha/{AR,CL,CO,PE,MX} $TEMP_PATH
fecha=$(date +"%Y%m")

 #Esto junta y ademas renombra
cat $TEMP_PATH/{AR,CL,CO,PE,MX}/*.csv > $TEMP_PATH/equifax_nse_$fecha.csv
echo put $TEMP_PATH/equifax_nse_$fecha.csv | sftp -i /home/rely/.ssh/equifax.key equifax@input-01:/equifax_homes
rm -r $TEMP_PATH
rm -r $TEMP_PATH/equifax_nse_$fecha.csv