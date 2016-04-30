#!/bin/bash

if [ $# -lt 2 ]; then
  echo "Please input correct form"
  echo "Correct format is ./run.sh \${INPUT_PATH} \${SEARCHED_WORD}"
  exit 1
fi

rm *.txt
hdfs dfs -rm -r -f HW1

hdfs dfs -mkdir HW1
hdfs dfs -mkdir HW1/input
hdfs dfs -put $1/* HW1/input

cd InvertedIndex
  ./compile.sh
  ./execute.sh
  cp output/part* ../output_invertedindex.txt
cd -

cd Search
  ./compile.sh
  ./execute.sh $2
  cp output/output_retrieval.txt ../
cd -
