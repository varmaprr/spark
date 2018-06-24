"# Spark"

This is repo i used to explore new libraries, tools, examples, and documentation focused on making it easier to build systems on top of the
Hadoop ecosystem and mainly focused on Spark processes.

This project is organized into modules. Modules may be independent or have
dependencies on other modules within spark. When possible, dependencies on
external projects are minimized.

## Modules

The following modules currently exist.

### Spark-Batch

The data module provides logical abstractions on top of storage subsystems (e.g.
HDFS) that let users think and operate in terms of records, datasets, and
dataset repositories. If you're looking to read or write records directly
to/from a storage system, the data module is for you.

## Examples

Example code demonstrating how to use Spark-batch

## Building

To build

```
mvn install
```

Steps to setup run in intellij

## Running in local

```
Download and store winutils.exe in local folder EX: C:\hadooop\bin.

set HADOOP_HOME = C:\hadooop\;

update PATH = %PATH%;%HADOOP_HOME%\bin;

Add winutils.exe

clone this repo and open in intellij

SparkJobRunner.scala is the main class.

Right click on SparkJobRunner.scala and run!!

```
