#!/bin/bash


source /home/rely/.bashrc
TEMP_PATH="/home/rely/tmp_data/equifax_homes/"
fecha=$(date +"%Y-%m")

mkdir $TEMP_PATH
/home/rely/hadoop/bin/hdfs dfs -copyToLocal /datascience/geo/NSEHomes/monthly/equifax/to_push/$fecha/{AR,CL,CO,PE,MX} $TEMP_PATH


cat $TEMP_PATH'AR/'*.csv > $TEMP_PATH"EQUIFAX_HOMES_AR_$fecha.csv"
cat $TEMP_PATH'CL/'*.csv > $TEMP_PATH"EQUIFAX_HOMES_CL_$fecha.csv"
cat $TEMP_PATH'CO/'*.csv > $TEMP_PATH"EQUIFAX_HOMES_CO_$fecha.csv"
cat $TEMP_PATH'PE/'*.csv > $TEMP_PATH"EQUIFAX_HOMES_PE_$fecha.csv"
cat $TEMP_PATH'MX/'*.csv > $TEMP_PATH"EQUIFAX_HOMES_MX_$fecha.csv"

#for pe in $(ls -1 $TEMP_PATH'country=PE/'); do mv $TEMP_PATH'country=PE/'$pe $TEMP_PATH"pii_$fecha"'_PE.csv'; done


#echo put $TEMP_PATH/equifax_nse_$fecha.csv | sftp -i /home/rely/.ssh/equifax.key equifax@input-01:/equifax_homes
#rm -r $TEMP_PATH
#rm -r $TEMP_PATH/
