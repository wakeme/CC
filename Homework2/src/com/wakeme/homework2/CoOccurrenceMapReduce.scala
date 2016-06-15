package com.wakeme.homework2

import java.io._
import java.nio.file._
import java.util.List
import java.util.ArrayList

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Map
import scala.util.matching._

object CoOccurrenceMapReduce {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <input> <output>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("co-occurrence-count").setMaster("local")
    val sc = new SparkContext(conf)
    
    // to save map result
    val coOccurCount: Map[String, Int] = Map()
    val ONE: Int = 1;
    
    // get each line of input file
    val article = sc.textFile(args(0))
    
    // convert string into lower-case, remove all punctuation
    // split the line into words
    val lines = article.map {_.toLowerCase().replaceAll("[^a-z\\s]", "").trim()}
    val words = lines.map { _.split("\\s+").filter(_ != "").toList }.collect()
    
    val rdd = sc.parallelize(words)
    var sorted = rdd.flatMap{line => 
      val coOccurCount: Map[String, Int] = Map()
      val occuredWords : List[String] = new ArrayList[String]()
      for (i <- 0 to line.length - 1) {
        if (!occuredWords.contains(line(i))) {
          for (j <- 0 to line.length - 1) {
            if (i != j) {
              val wp = line(i) + "_" + line(j)
              if (coOccurCount.get(wp) == None) {
                coOccurCount.update(wp, ONE)
              } else {
                coOccurCount.update(wp, coOccurCount.get(wp).get + ONE)
              }
              occuredWords.add(line(i))
            }
          }
        }
      }
      occuredWords.clear()
      coOccurCount
    }.reduceByKey(_+_).collect().sorted
    sc.parallelize(sorted).saveAsTextFile(args(1))
  }
}