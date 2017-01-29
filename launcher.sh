#!/bin/bash


# Change this to your netid
netid=$2

#
# Root directory of your project
#PROJDIR=$HOME/CS6378/Project1
PROJDIR=$HOME
#
# This assumes your config file is named "config.txt"
# and is located in your project directory
#
CONFIG=$PROJDIR/$1

#
# Directory your java classes are in
#
BINDIR=$PROJDIR

#
# Your main project class
#
PROG=ChandyLamport

n=1

cat $CONFIG | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
    read i 
    #echo $i
    n=$( echo $i | awk '{print $1}')
    j=0
    #echo $n
    while [ $j -lt $n ]
    do
        read line
        #echo $line
        host=$( echo $line | awk '{ print $2 }' )
        echo $host
        #echo $BINDIR $PROG
	ssh $netid@$host java -cp $BINDIR $PROG $n &
        j=$(( j + 1 ))
    done
   
)