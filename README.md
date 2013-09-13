Cipex-Analyse
=============

Introduction
------------

Cipex-Analyse is a Hadoop Job for analysing Cipex output tables.

Installation
------------

    cd cipex-analyse
    mvn install

Usage
-----

The command line application is executed by typing

    hadoop -jar
      target/cipex-analyse-1.0-SNAPSHOT-jar-with-dependencies.jar 
      -d /path/to/hdfs/directory/

where

    -d,--dir <arg>    HDFS directory containing cipex output data.

Additional hadoop parameters must be defined after the jar parameter, e.g.
setting the maximum number of tasks that should run in parallel:

    hadoop jar
      target/cipex-1.0-SNAPSHOT-jar-with-dependencies.jar
      -Dmapred.tasktracker.map.tasks.maximum=2
      -d /path/to/hdfs/input/directory
