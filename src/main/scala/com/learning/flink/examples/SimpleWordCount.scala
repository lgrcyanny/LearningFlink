package com.learning.flink.examples

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

/**
  * Created by lgrcyanny on 17/5/7.
  */
object SimpleWordCount {
  case class WordWithCount(word: String, count: Int)
  def main(args: Array[String]): Unit = {
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException("Can't get port from args: --port <port>")
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')
    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w => w.split(" ") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)
    env.execute("Socket Window WordCount")
  }

}
