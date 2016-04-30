#!/bin/bash

if [ $# -lt 1 ]; then
  echo "Please input one word to search"
  exit 1
fi

rm output_retrieval.txt

cd Search
  ./compile.sh
  ./execute.sh $1
  cp output/output_retrieval.txt ../
cd -
