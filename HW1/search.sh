#!/bin/bash

if [ $# -lt 2 ]; then
  echo "Please input correct form"
  echo "Correct format is ./search.sh \${SEARCHED_WORD} enable/disable"
  exit 1
fi

rm output_retrieval.txt

if [ $2 == "enable" ]; then
  cd Filter
    ./compile.sh
    ./execute.sh $1
  cd -
fi
cd Search
  ./compile.sh
  ./execute.sh $1 $2
  cp output/output_retrieval.txt ../
cd -
