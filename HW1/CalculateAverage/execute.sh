# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/CalculateAverage/Output/
#hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage /user/shared/CalculateAverage/Input /user/TA/CalculateAverage/Output
#hdfs dfs -cat /user/TA/CalculateAverage/Output/part-*

your_hadoop_output_directory=HW1/output
INPUT=HW1/input
hdfs dfs -rm -r ${your_hadoop_output_directory}
hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage $INPUT ${your_hadoop_output_directory}

rm output/*
hdfs dfs -cat ${your_hadoop_output_directory}/part-*
hdfs dfs -get ${your_hadoop_output_directory}/part-* output/
