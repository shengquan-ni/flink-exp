package org.apache.flink.streaming.util.recovery

import org.apache.flink.api.java.tuple
import org.apache.flink.runtime.recovery.AbstractLogStorage

class EmptyLogStorage(logName: String) extends AbstractLogStorage(logName)  {
  override def write(record: AbstractLogStorage.LogRecord): Unit = {}

  override def getStepCursor: Long = 0

  override def commit(): Unit = {}

  override def getLogs: Iterable[AbstractLogStorage.LogRecord] = Iterable.empty

  override def clear(): Unit = {}

  override def release(): Unit = {}

  override def getLoggedWindows: Array[tuple.Tuple2[java.lang.Long, java.lang.Long]] = Array.empty

  override def getLoggedTimers: Array[tuple.Tuple2[java.lang.Long, java.lang.Long]] = Array.empty

  override def truncateLog(): Unit = {}
}
