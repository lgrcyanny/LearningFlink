package com.learning.flink.examples

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object AverageSensorReadings {

  case class SensorReading(id: String, timestamp: Long, temperature: Double)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val numbers: DataStream[(Int, Int, Int)] = env.fromElements((1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))
    numbers.print()
    val resultStream: DataStream[(Int, Int, Int)] = numbers.keyBy(0).sum(1)
    resultStream.print()
    env.execute()
  }

}
