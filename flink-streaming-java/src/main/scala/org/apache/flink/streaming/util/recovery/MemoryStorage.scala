package org.apache.flink.streaming.util.recovery

import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.runtime.recovery.AbstractLogStorage
import org.apache.flink.runtime.recovery.AbstractLogStorage.LogRecord
import org.apache.flink.streaming.util.recovery.MemoryStorage.globalMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object MemoryStorage{
  val globalMap = new mutable.HashMap[String, MemoryStorage]()
}

class MemoryStorage(logName: String) extends AbstractLogStorage(logName)  {

  var logs = new ArrayBuffer[LogRecord]()
  var loggedWindows = new ArrayBuffer[tuple.Tuple2[java.lang.Long, java.lang.Long]]()
  var loggedTimers = new ArrayBuffer[tuple.Tuple2[java.lang.Long, java.lang.Long]]()
  var loggedStepCursor:Long = 0

  if(globalMap.contains(logName)){
    logs = globalMap(logName).logs
    loggedWindows = globalMap(logName).loggedWindows
    loggedTimers = globalMap(logName).loggedTimers
    loggedStepCursor = globalMap(logName).loggedStepCursor
  }
  globalMap(logName) = this

  override def write(record: LogRecord): Unit = {
    record match{
      case AbstractLogStorage.DPCursor(idx) =>
        logs.append(record)
        loggedStepCursor = idx
      case AbstractLogStorage.UpdateStepCursor(step) =>
        loggedStepCursor = step
      case AbstractLogStorage.TimerStart(startTime, startCursor) =>
        loggedTimers.append(new Tuple2(startTime, startCursor))
      case AbstractLogStorage.WindowStart(startTime, startCursor) =>
        loggedWindows.append(new Tuple2(startTime, startCursor))
      case AbstractLogStorage.ControlRecord(controlName, controlArgs) =>
        logs.append(record)
      case AbstractLogStorage.ChannelOrder(inputNum, newChannelID, lastChannelRecordCount) =>
        logs.append(record)
    }
  }

  override def getStepCursor: Long = loggedStepCursor

  override def commit(): Unit = {}

  override def getLogs: Iterable[LogRecord] = logs

  override def clear(): Unit = {
    logs = new ArrayBuffer[LogRecord]()
    loggedWindows = new ArrayBuffer[tuple.Tuple2[java.lang.Long, java.lang.Long]]()
    loggedTimers = new ArrayBuffer[tuple.Tuple2[java.lang.Long, java.lang.Long]]()
    loggedStepCursor = 0
  }

  override def release(): Unit = {}

  override def getLoggedWindows: Array[tuple.Tuple2[java.lang.Long, java.lang.Long]] = loggedWindows.toArray

  override def getLoggedTimers: Array[tuple.Tuple2[java.lang.Long, java.lang.Long]] = loggedTimers.toArray
}
