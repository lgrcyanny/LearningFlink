package com.learning.flink.examples

import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object UsingKeyState {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val conf = new Configuration()
    conf.setString("keyWord", "flink")
    env.getConfig.setGlobalJobParameters(conf)
    val input = env.fromElements("I love flink", "banannas is great", "flink is great")
    val words = input.flatMap(_.split(" ")).map(w => (w, 1))
    val wordsCount = words.keyBy(_._1).map(new RollingSum)
    wordsCount.print()
    env.execute("UsingKeyState")
  }

  class RollingSum extends RichMapFunction[(String, Int), (String, Long)] {
    var sumState: ValueState[Long] = _
    override def open(parameters: Configuration): Unit = {
      val stateDescriptor = new ValueStateDescriptor[Long]("sumState", createTypeInformation[Long])
      sumState = getRuntimeContext.getState(stateDescriptor)
    }

    override def map(in: (String, Int)): (String, Long) = {
      val sumValue = sumState.value()
      val newSum = if (sumValue == null.asInstanceOf[Long]) {
        in._2
      } else {
        sumValue + in._2
      }
      sumState.update(newSum)
      (in._1, newSum)
    }
  }

}
