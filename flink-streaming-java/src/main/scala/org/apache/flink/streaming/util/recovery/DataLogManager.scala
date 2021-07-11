package org.apache.flink.streaming.util.recovery

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo
import org.apache.flink.runtime.io.network.api.CheckpointBarrier
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent
import org.apache.flink.runtime.recovery.AbstractLogStorage.ChannelOrder
import org.apache.flink.runtime.recovery.{AbstractLogManager, AsyncLogWriter, RecoveryUtils, StepCursor}
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput
import org.apache.flink.streaming.runtime.streamrecord.StreamElement
import org.apache.flink.streaming.util.recovery.DataLogManager._

import scala.collection.mutable

object DataLogManager{
  val PROCESSED_NOTHING = 0
  val PROCESSED_RECORD = 1
  val PROCESSED_EVENT = 2
}


class DataLogManager(logWriter: AsyncLogWriter, val stepCursor: StepCursor) extends AbstractLogManager {

  private val prevOrders = new mutable.Queue[(Int,InputChannelInfo)]()
  private val prevCounts = new mutable.Queue[Int]()
  private var lastGate:Int = -1
  private var lastChannel: InputChannelInfo = _
  private var recordCount = 0
  private var lastBuffer:BufferOrEvent = _

  private var checkpointLock:AnyRef = _

  def setCheckpointLock(obj:AnyRef): Unit = {
    checkpointLock = obj
  }

  def getCheckpointLock:AnyRef = checkpointLock
  def getName:String = logWriter.storage.name

  logWriter.storage.getLogs.foreach {
    case fs: ChannelOrder =>
      if(lastGate == -1){
        lastGate = fs.inputNum
        lastChannel = fs.newChannelID
      }else{
        prevCounts.enqueue(fs.lastChannelRecordCount)
        prevOrders.enqueue((fs.inputNum,fs.newChannelID))
      }
    case other => //skip
  }

  private val inners = new mutable.HashMap[Int, DataLogManagerInner[_]]()
  private val ofoMap = new mutable.HashMap[Int, mutable.HashMap[InputChannelInfo, mutable.Queue[Either[BufferOrEvent, (StreamElement, DataOutput[_])]]]]()
  private val spilled = new mutable.Queue[(Int, InputChannelInfo, Either[BufferOrEvent, (StreamElement, DataOutput[_])])]()

  stepCursor.onComplete(() =>{
    if(ofoMap.nonEmpty){
      ofoMap.foreach{
        case (gate, channelMap) =>
          channelMap.foreach{
            case (channel, queue) =>
              queue.foreach(elem => spilled.enqueue((gate, channel, elem)))
          }
      }
      ofoMap.clear()
    }
  })

  def registerInput[T](handler:ThrowingTriConsumer[InputChannelInfo,StreamElement, DataOutput[T], _],eventHandler: ThrowingConsumer[BufferOrEvent, _]): Int ={
    val token = inners.size
    inners(token) = new DataLogManagerInner(handler,eventHandler)
    token
  }


  def getBuffer:BufferOrEvent = lastBuffer

  def inputData[T](token:Int, channel:InputChannelInfo, elem:StreamElement, dataOutput: DataOutput[T]): Int ={
    if(!stepCursor.isRecoveryCompleted){
      val entry1 = ofoMap.getOrElseUpdate(token, mutable.HashMap())
      val entry2 = entry1.getOrElseUpdate(channel, mutable.Queue())
      entry2.enqueue(Right(elem, dataOutput))
      recoverUpstream()
    }else if(spilled.nonEmpty){
      spilled.enqueue((token, channel, Right(elem, dataOutput)))
      val (altGate, altChannel, altPayload) = spilled.dequeue()
      preprocessInput(altGate, altChannel)
      processElement(altGate, altChannel, altPayload)
    }else {
      preprocessInput(token, channel)
      inners(token).asInstanceOf[DataLogManagerInner[T]].inputDataRecord(channel, elem, dataOutput)
      PROCESSED_RECORD
    }
  }

  def inputEvent[T](token:Int, channel:InputChannelInfo, elem:BufferOrEvent): Int ={
    if(elem.getEvent.isInstanceOf[CheckpointBarrier]){
      if(RecoveryUtils.needPrint(RecoveryUtils.PRINT_DIRECT_CALL)) {
        println(s"${logWriter.storage.name} process event = $elem directly")
      }
      inners(token).eventHandler.accept(elem)
      return PROCESSED_EVENT
    }
    if(!stepCursor.isRecoveryCompleted) {
      val entry1 = ofoMap.getOrElseUpdate(token, mutable.HashMap())
      val entry2 = entry1.getOrElseUpdate(channel, mutable.Queue())
      entry2.enqueue(Left(elem))
      recoverUpstream()
    }else if(spilled.nonEmpty){
      spilled.enqueue((token, channel, Left(elem)))
      val (altGate, altChannel, altPayload) = spilled.dequeue()
      preprocessInput(altGate, altChannel)
      processElement(altGate, altChannel, altPayload)
    }else{
      preprocessInput(token, channel)
      inners(token).inputEvent(channel, elem)
      lastBuffer = elem
      PROCESSED_EVENT
    }
  }

  def recoverUpstream[T]():Int = {
    if(ofoMap.contains(lastGate) && ofoMap(lastGate).contains(lastChannel) && ofoMap(lastGate)(lastChannel).nonEmpty){
      val ret = processElement(lastGate, lastChannel, ofoMap(lastGate)(lastChannel).dequeue())
      recordCount += 1
      if(prevCounts.nonEmpty && recordCount == prevCounts.head){
        prevCounts.dequeue()
        val (g, c) = prevOrders.dequeue()
        lastGate = g
        lastChannel = c
        recordCount = 0
      }
      ret
    }else if(spilled.nonEmpty){
      val (altGate, altChannel, altPayload) = spilled.dequeue()
      preprocessInput(altGate, altChannel)
      processElement(altGate, altChannel, altPayload)
    }else{
      PROCESSED_NOTHING
    }
  }


  def processElement[T](gate:Int, channel:InputChannelInfo, either: Either[BufferOrEvent, (StreamElement, DataOutput[_])]): Int ={
    either match {
      case Left(a) =>
        inners(gate).inputEvent(channel, a)
        lastBuffer = a
        PROCESSED_EVENT
      case Right((s,d)) =>
        inners(gate).asInstanceOf[DataLogManagerInner[T]].inputDataRecord(channel, s, d.asInstanceOf[DataOutput[T]])
        PROCESSED_RECORD
    }
  }

  def preprocessInput(token:Int, channel:InputChannelInfo): Unit ={
    if(lastGate != token || lastChannel != channel){
      //println(s"${logWriter.storage.name} pushing log record: ${ChannelOrder(token, channel, recordCount)}")
      logWriter.addLogRecord(ChannelOrder(token, channel, recordCount))
      lastGate = token
      lastChannel = channel
      recordCount = 1
    }else{
      recordCount += 1
    }
  }

  class DataLogManagerInner[T](dataHandler: ThrowingTriConsumer[InputChannelInfo,StreamElement, DataOutput[T], _],
                               val eventHandler: ThrowingConsumer[BufferOrEvent, _]){

    def inputDataRecord(channel:InputChannelInfo, elem:StreamElement, output: DataOutput[T]): Unit ={
      if(RecoveryUtils.needPrint(RecoveryUtils.PRINT_PROCESS)) {
        println(s"${logWriter.storage.name} process data = $elem when step = ${stepCursor.getCursor} from $channel")
      }
      dataHandler.accept(channel, elem, output)
      stepCursor.advance()
    }

    def inputEvent(channel: InputChannelInfo, elem:BufferOrEvent): Unit ={
      if(RecoveryUtils.needPrint(RecoveryUtils.PRINT_PROCESS)) {
        println(s"${logWriter.storage.name} process event = $elem when step = ${stepCursor.getCursor} from $channel")
      }
      eventHandler.accept(elem)
      stepCursor.advance()
    }
  }

}
