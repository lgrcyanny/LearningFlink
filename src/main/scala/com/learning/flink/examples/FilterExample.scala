package com.learning.flink.examples

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object FilterExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val conf = new Configuration()
    conf.setString("keyWord", "flink")
    env.getConfig.setGlobalJobParameters(conf)
    val input = env.fromElements("I love flink", "banannas", "flink is great")
    input.filter(new MyFilterFunction).print
    env.execute("FilterExample")
  }

  class MyFilterFunction extends RichFilterFunction[String] {
    var keyWord = ""

    override def open(parameters: Configuration): Unit = {
      val globalParams = getRuntimeContext.getExecutionConfig.getGlobalJobParameters
      val globalConf = globalParams.asInstanceOf[Configuration]
      keyWord = globalConf.getString("keyWord", null)
    }

    override def filter(value: String): Boolean = {
      value.contains(keyWord)
    }
  }

}
