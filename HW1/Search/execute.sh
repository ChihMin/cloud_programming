# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/CalculateAverage/Output/
#hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage /user/shared/CalculateAverage/Input /user/TA/CalculateAverage/Output
#hdfs dfs -cat /user/TA/CalculateAverage/Output/part-*
if [ $# -lt 1 ]; then
  echo "Please input search word"
  exit 1
fi

your_hadoop_output_directory=HW1/output_invertedindex
#INPUT=HW1/input
INPUT=$your_hadoop_output_directory
your_hadoop_output_directory=HW1/output_retrieval
hdfs dfs -rm -r ${your_hadoop_output_directory}
hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage $INPUT ${your_hadoop_output_directory} $1

rm output/*
# hdfs dfs -cat ${your_hadoop_output_directory}/part-*
hdfs dfs -get ${your_hadoop_output_directory}/out* output/
hdfs dfs -get ${your_hadoop_output_directory}/part* output/
hdfs dfs -get ${your_hadoop_output_directory}/doc* output/
