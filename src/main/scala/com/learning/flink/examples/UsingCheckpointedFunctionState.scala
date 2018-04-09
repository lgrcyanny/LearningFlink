package com.learning.flink.examples

import java.util

import com.learning.flink.examples.UsingKeyState.RollingSum
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.{CheckpointedFunction, ListCheckpointed}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.collection.JavaConverters._

object UsingCheckpointedFunctionState {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val conf = new Configuration()
    conf.setString("keyWord", "flink")
    env.getConfig.setGlobalJobParameters(conf)
    env.getConfig.setGlobalJobParameters(conf)
    val input = env.fromElements("I love flink", "banannas is great", "flink is great")
    val words = input.flatMap(_.split(" ")).map(w => (w, 1))
    val wordsCount: DataStream[(String, Long, Long)] = words.keyBy(_._1).map(new RollingSum)
    wordsCount.print()
    env.execute("UsingRollingPartitionSum")
  }

  class RollingSum extends RichMapFunction[(String, Int), (String, Long, Long)] with CheckpointedFunction {
    import scala.collection.JavaConverters._
    var opSum: Long = 0
    var keyedSumState: ValueState[Long] = _
    var opSumState: ListState[Long] = _

    override def map(value: (String, Int)): (String, Long, Long) = {
      opSum += value._2
      val keySum = keyedSumState.value() + value._2
      keyedSumState.update(keySum)
      (value._1, keySum, opSum)
    }

    override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
      opSumState.clear()
      opSumState.add(opSum)
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {
      val keySumDescriptor = new ValueStateDescriptor[Long]("keyedSum", createTypeInformation[Long])
      keyedSumState = context.getKeyedStateStore.getState(keySumDescriptor)

      val opStateDescriptor = new ListStateDescriptor[Long]("opSum", createTypeInformation[Long])
      opSumState = context.getOperatorStateStore.getListState(opStateDescriptor)
      opSum = opSumState.get().asScala.sum
    }
  }

}
