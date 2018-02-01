package com.learning.flink.examples

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import scala.collection.JavaConversions._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by lgrcyanny on 17/5/7.
  */
object SimpleWordCount {

  def main(args: Array[String]): Unit = {
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException("Can't get port from args: --port <port>")
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // get input data by connecting to the socket
    val text: DataStream[String] = env.socketTextStream("localhost", port, '\n')
    case class WordWithCount(word: String, count: Long)
    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w: String => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.milliseconds(10), Time.milliseconds(1))
      .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)
    println(env.getExecutionPlan)
    env.execute("Socket Window WordCount")
  }

}
