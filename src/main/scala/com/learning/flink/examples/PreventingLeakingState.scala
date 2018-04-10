package com.learning.flink.examples

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object PreventingLeakingState {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val conf = new Configuration()
    conf.setString("keyWord", "flink")
    env.getConfig.setGlobalJobParameters(conf)
    val input = env.fromElements("I love flink", "banannas is great", "flink is great")
    val words = input.flatMap(_.split(" ")).map(w => (w, 1))
    val wordsCount = words.keyBy(_._1).process(new RollingSumProcessor)
    wordsCount.print()
    env.execute("PreventingLeakingState")
  }

  class RollingSumProcessor extends ProcessFunction[(String, Int), (String, Long)] {
    var sumState: ValueState[Long] = _
    var lastTimerState: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
      val stateDescriptor = new ValueStateDescriptor[Long]("sumState", createTypeInformation[Long])
      sumState = getRuntimeContext.getState(stateDescriptor)

      val timerDescriptor = new ValueStateDescriptor[Long]("timerState", createTypeInformation[Long])
      lastTimerState = getRuntimeContext.getState(timerDescriptor)
    }


    override def processElement(value: (String, Int),
                                ctx: ProcessFunction[(String, Int), (String, Long)]#Context,
                                out: Collector[(String, Long)]): Unit = {
      // get timestamp of current value and add one hour
      // TODO(lgrcyanny) ctx.timestamp always null
      val checkTimestamp = if (ctx.timestamp() == null.asInstanceOf[java.lang.Long]) {
        println("ctx.timestamp is null")
        System.currentTimeMillis()
      } else {
        ctx.timestamp().longValue() + (3600 * 1000L)
      }
      // get timestamp of last timer
      val lastTimer = lastTimerState.value()
      // check if check timestamp > last timer and register new timer
      if (lastTimer == null.asInstanceOf[Long] ||
        checkTimestamp > lastTimer) {
        // register new timer
        ctx.timerService().registerEventTimeTimer(checkTimestamp)
        // update timestamp of last timer
        lastTimerState.update(checkTimestamp)
      }

      val sumValue = sumState.value()
      val newSum = if (sumValue == null.asInstanceOf[Long]) {
        value._2
      } else {
        sumValue + value._2
      }
      sumState.update(newSum)
      out.collect((value._1, newSum))
    }


    override def onTimer(timestamp: Long,
                         ctx: ProcessFunction[(String, Int), (String, Long)]#OnTimerContext,
                         out: Collector[(String, Long)]): Unit = {
      val lastTimer = lastTimerState.value()
      if (lastTimer != null.asInstanceOf[Long] && lastTimer == timestamp) {
        sumState.clear()
        lastTimerState.clear()
      }
    }
  }

}
