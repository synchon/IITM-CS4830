# -*- coding: utf-8 -*-

import argparse, sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # Build the CLI
    parser = argparse.ArgumentParser(description="Word Count")
    parser.add_argument("input_file", type=str,
                        help="path to input file")
    parser.add_argument("output_path", type=str,
                        help="path to output")

    args = parser.parse_args()

    # Build a new SparkConf and set the App name
    conf = SparkConf().setAppName("Word Count")

    # Build a new SparkContext
    sc = SparkContext(conf=conf)
    
    # Change the log level of the SparkContext
    sc.setLogLevel("ERROR")

    if args.input_file and args.output_path:
        word_count = sc.textFile(args.input_file).flatMap(
            lambda line: line.split(" ")).map(
            lambda word: (word.strip(",").strip("."), 1)).reduceByKey(
            lambda a, b: a + b)
                            
        word_count.saveAsTextFile(args.output_path)
    
    else:
        args.print_usage()
        sys.exit(1)
