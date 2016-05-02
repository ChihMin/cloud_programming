#!/bin/bash

if [ $# -lt 3 ]; then
  echo "Please input correct form"
  echo "Correct format is ./run.sh \${INPUT_PATH} \${SEARCHED_WORD} enable/disable"
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

./search.sh $2 $3
