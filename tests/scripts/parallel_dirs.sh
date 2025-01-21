#!/bin/bash

mkdir /tmp/fsfs/{00..19}

mkdir /tmp/fsfs/00/{01..10} &
mkdir /tmp/fsfs/01/{10..19} &
mkdir /tmp/fsfs/02/{20..29} &
mkdir /tmp/fsfs/03/{30..39} &
mkdir /tmp/fsfs/04/{40..49} &
mkdir /tmp/fsfs/05/{50..59} &
mkdir /tmp/fsfs/06/{60..69} &
mkdir /tmp/fsfs/07/{70..79} &
mkdir /tmp/fsfs/08/{80..89} &
mkdir /tmp/fsfs/09/{90..99} &
mkdir /tmp/fsfs/10/{100..109} &
mkdir /tmp/fsfs/11/{110..119} &
mkdir /tmp/fsfs/12/{120..129} &
mkdir /tmp/fsfs/13/{130..139} &
mkdir /tmp/fsfs/14/{140..149} &
mkdir /tmp/fsfs/15/{150..159} &
mkdir /tmp/fsfs/16/{160..169} &
mkdir /tmp/fsfs/17/{170..179} &
mkdir /tmp/fsfs/18/{180..189} &
mkdir /tmp/fsfs/19/{190..199}

wait

if [ $(tree /tmp/fsfs | grep dir | cut -d' ' -f 1) -eq '221' ]; then 
    echo Ok
else
    echo Fail
fi

rm -r /tmp/fsfs/{00..19}