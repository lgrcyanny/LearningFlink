package com.learning.flink.examples

/**
  * Created by lgrcyanny on 17/5/9.
  */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("Hello")
    println(-1 >>> 2)
    println(-1 >> 2)
  }
}
