#!/bin/bash
###############################################################################################
#  Description  :   This script can copy hive tables (full tables or select partitions) across S3 and HDFS (can also copy within S3 & S3 or HDFS & HDFS) in AWS. 
#  Date         :   10/2018
#  Author       :   Rodrigo Sejas
#  Usage        :   sh hdfs_copy.sh  -s [source_schema] -t [target_schema]                                                  will copy only tables that are in target schema that are also in source schema into target schema. 
#                   sh hdfs_copy.sh  -s [source_schema] -t [target_schema] -r ["regex_pattern"]                              can copy specific partitions of tables that are in target schema and source schema into target schema.
#                   sh hdfs_copy.sh  -s [source_schema] -t [target_schema] -r [regex_pattern] -l [table_list_file_nm_path]  can copy specific partitions and only select tables from source to target. Assumes table structures are present in target schema.
#
# 					-r [regex_patter] requires a regular expression to select 1 or a range of partitions based on the values of your partition column. 
#					regex needs to be enclosed in double quotes "".	
#					-l [table_list_file_nm_path] requires full path + file name.
#
# TO DO			: create a check to make sure all tables are present in target schema.
#				   
###############################################################################################

source_schema=
target_schema=
regex_pattern=
table_list_file_nm=
temp_list_file_path=$(pwd)/


while   getopts s:t:r:l:e: parm
do      case    $parm in
        (s)     source_schema="$OPTARG";;
        (t)     target_schema="$OPTARG";;
        (r)     regex_pattern="$OPTARG";;
        (l)     table_list_file_nm="$OPTARG";;
        (?)     printf  'Usage mtx2h.shl [-s [/s3_source_path/] -t [/hdfs_target_path/] \n';
				printf  'Usage mtx2h.shl [-s [/s3_source_path/] -t [/hdfs_target_path/] -r [regex_pattern] \n';
				printf  'Usage mtx2h.shl [-s [/s3_source_path/] -t [/hdfs_target_path/] -r [regex_pattern] [-l <table_list_file_nm>] \n Exiting.......\n';
                exit -1;;
        esac
done
shift $(($OPTIND - 1))

if [ -z $source_schema ];
        then echo -e "\nERROR\nRequired parameter missing, please supply source_path -s <source_path>\n\n"
            exit 1
fi

if [ -z $target_schema ];
        then echo -e "\nERROR\nRequired parameter missing, please supply target_path -t <target_path>\n\n"
            exit 1
fi

source_schema_path=`hive -e "show create schema $source_schema;" | grep '://' |sed "s/'//g"|sed "s/ //g"`
target_schema_path=`hive -e "show create schema $target_schema;" | grep '://' |sed "s/'//g"|sed "s/ //g"`
echo
echo source_schema_path : $source_schema_path
echo target_schema_path : $target_schema_path
echo regex_pattern : $regex_pattern
echo table_list_file_name : $table_list_file_nm


#Cannot copy S3 _$folder$ into hdfs. Will cause java error.
#if no regex pattern was given then regex_pattern must contain regular expression to exclude all _$folder$ paths
if [ -z $regex_pattern ];
		then regex_pattern='^((?!_\$folder\$).)*$'
else
	regex_pattern=$regex_pattern/            #assuming regular expression does not take the closing file path (/) into account.
fi

#if no table list file was given, then generate list of tables from target schema.
no_list_file_given=
if [ -z $table_list_file_nm ];
           then table_list_file_nm=${temp_list_file_path}${target_schema}_table_list.txt
				no_list_file_given=yes
                hive -e "set hive.cli.print.header=false;use $target_schema; show tables;" > $table_list_file_nm
fi

#file will prefixed table paths to be copied from source.
filtered_source_files_to_cp=${temp_list_file_path}S3_HDFS_copy_temp_table_list.txt
while read line;
do echo ${source_schema_path}/$line/
done < $table_list_file_nm > $filtered_source_files_to_cp

echo
echo "Starting Copy from Source to Target...";
echo

#s3-dist-cp -Dmapreduce.fileoutputcommitter.algorithm.version=2 --multipartUploadChunkSize=128 --src $source_schema_path/ --dest $target_schema_path/ --srcPrefixesFile  file://$filtered_source_files_to_cp --srcPattern=.*$regex_pattern.*
s3-dist-cp --multipartUploadChunkSize=128 --src $source_schema_path/ --dest $target_schema_path/ --srcPrefixesFile  file://$filtered_source_files_to_cp --srcPattern=.*$regex_pattern.*

echo
echo COPY Finished. Repairing tables now...
echo 

#repair tables. hdfs metastores will not be aware of new partitions until tables are repaired. 
while read line;
	do hive -e "set hive.cli.print.header=false;msck repair table $target_schema.$line;"
done < $table_list_file_nm

#file cleanup
echo
rm -f $filtered_source_files_to_cp

if [ ! -z $no_list_file_given ];
      then rm -f $table_list_file_nm
fi

echo
echo "Tables have been repaired. Copy Job complete.";
echo