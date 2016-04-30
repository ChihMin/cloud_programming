#!/bin/bash

if [ $# -lt 1 ]; then
  echo "Please input one word to search"
  exit 1
fi

rm *.txt

hdfs dfs -mkdir HW1
hdfs dfs -mkdir HW1/input
hdfs dfs -put input/* HW1/input

cd InvertedIndex
  ./compile.sh
  ./execute.sh
  cp output/part* ../output_invertedindex.txt
cd -

cd Search
  ./compile.sh
  ./execute.sh $1
  cp output/output_retrieval.txt ../
cd -
