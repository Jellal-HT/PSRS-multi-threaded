#!/bin/bash

KEYS=( 16000000 32000000 64000000 128000000 256000000 512000000 )

mkdir -p testResult

for threadNum in 1 2 4 6 8
do
	for size in "${KEYS[@]}"
	do
		for time in {1..5}
		do
			echo "Using ${threadNum} to test ${size} in $time th time"
			./psrs.o $size $hreadNum > ./testResult/out"$threadNum-$size-$time".txt
		done
	done
done
