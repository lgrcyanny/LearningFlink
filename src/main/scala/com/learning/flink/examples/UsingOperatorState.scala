package com.learning.flink.examples

import java.util
import java.util.Collections

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.JavaConverters._
import org.apache.flink.streaming.api.scala._

object UsingOperatorState {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val conf = new Configuration()
    env.getConfig.setGlobalJobParameters(conf)
    val input = env.fromElements(1L, 3L, 4L, 5L, 8L, 1L)
    val count = input.map(new RollingPartitionSum)
    count.print()
    env.execute("UsingRollingPartitionSum")
  }

  class RollingPartitionSum extends MapFunction[Long, Long] with ListCheckpointed[java.lang.Long] {
    import scala.collection.JavaConverters._
    var sum: Long = 0

    override def map(t: Long): Long = {
      sum += t
      sum
    }

    override def restoreState(list: util.List[java.lang.Long]): Unit = {
      list.asScala.foreach(x => sum += x)
    }

    override def snapshotState(l: Long, l1: Long): util.List[java.lang.Long] = {
      util.Collections.singletonList(sum)
    }
  }

}
