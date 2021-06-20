package org.apache.flink.runtime.recovery

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors, LinkedBlockingQueue}

import org.apache.flink.runtime.event.AbstractEvent
import org.apache.flink.runtime.io.network.buffer.BufferConsumer
import org.apache.flink.shaded.guava18.com.google.common.collect.Queues
import org.apache.flink.runtime.recovery.AbstractLogStorage._
import org.apache.flink.runtime.recovery.AsyncLogWriter.{CursorUpdate, NetworkOutputElem, OutputElem, ShutdownOutput}
import org.apache.flink.util.function.TriConsumer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AsyncLogWriter{
  sealed trait NetworkOutputElem
  case object ShutdownOutput extends NetworkOutputElem
  case class CursorUpdate(cursor:Long) extends NetworkOutputElem
  sealed trait OutputElem extends NetworkOutputElem{
    val cursor:Long
  }
  case class OutputBuffer(cursor:Long, idx:Int, payload:BufferConsumer, length:Int, finish:Boolean) extends OutputElem
}

class AsyncLogWriter(val storage:AbstractLogStorage) {
  private var persistedStepCursor = storage.getStepCursor
  private var cursorUpdated = false
  val logRecordQueue: LinkedBlockingQueue[LogRecord] = Queues.newLinkedBlockingQueue()
  private val shutdownFuture = new CompletableFuture[Void]()
  private val outputCallbackMap = new mutable.ArrayBuffer[TriConsumer[BufferConsumer, Integer, java.lang.Boolean]]()
  private val outputMailbox:LinkedBlockingQueue[NetworkOutputElem] = Queues.newLinkedBlockingQueue()

  def addLogRecord(logRecord: LogRecord): Unit = {
    logRecordQueue.put(logRecord)
  }

  def addOutput(elem:NetworkOutputElem): Unit ={
    //System.out.println(storage.name+" pushed "+elem)
    outputMailbox.put(elem)
  }


  def takeCheckpoint():Unit = {
    logRecordQueue.put(TruncateLog)
  }

  def shutdown(): CompletableFuture[Void] ={
    logRecordQueue.put(ShutdownWriter)
    shutdownFuture
  }

  def registerOutputCallback(outputCallback:TriConsumer[BufferConsumer, Integer, java.lang.Boolean]):Int = {
    val key = outputCallbackMap.size
    outputCallbackMap.append(outputCallback)
    key
  }

  private val outputExecutor:ExecutorService = Executors.newSingleThreadExecutor
  outputExecutor.submit(new Runnable {
    override def run(): Unit = {
      var myCursor = 0L
      var isEnded = false
      val myStash = new mutable.Queue[OutputElem]()
      try{
        while(!isEnded){
          outputMailbox.take() match {
            case AsyncLogWriter.ShutdownOutput =>
              isEnded = true
            case AsyncLogWriter.CursorUpdate(cursor) =>
              myCursor = cursor
              while(myStash.nonEmpty && myCursor >= myStash.head.cursor){
                myStash.dequeue() match {
                  case AsyncLogWriter.OutputBuffer(_, idx, payload, length, finish) =>
                    outputCallbackMap(idx).accept(payload, length, finish)
                }
              }
              //System.out.println(storage.name+": queue size after updating cursor to ="+myCursor+" is "+myStash.size)
            case elem @ AsyncLogWriter.OutputBuffer(cursor, idx, payload, length, finish) =>
              if(myCursor >= cursor){
                outputCallbackMap(idx).accept(payload, length, finish)
                //System.out.println(storage.name+" directly process since "+myCursor+" >= "+cursor)
              }else{
                myStash.enqueue(elem)
                //System.out.println(storage.name+" put in queue since "+myCursor+" < "+cursor)
              }
          }
        }
        outputCallbackMap.clear()
        outputMailbox.clear()
        shutdownFuture.complete(null)
      }catch{
        case e:Throwable =>
          e.printStackTrace()
      }
    }
  })

  private val loggingExecutor: ExecutorService = Executors.newSingleThreadExecutor
    loggingExecutor.submit(new Runnable() {
      def run(): Unit = {
        try {
          //Thread.currentThread().setPriority(Thread.MAX_PRIORITY)
          var isEnded = false
          val buffer = new util.ArrayList[LogRecord]()
          addOutput(CursorUpdate(persistedStepCursor))
          while (!isEnded) {
            logRecordQueue.drainTo(buffer)
            //if(storage.name == "exampleJob-0f02-0")System.out.println("drained = "+buffer.size())
            if (buffer.isEmpty) {
              // instead of using Thread.sleep(200),
              // we wait until 1 record has been pushed into the queue
              // then write this record and commit
              // during this process, we accumulate log records in the queue
              val logRecord = logRecordQueue.take()
              if (logRecord == ShutdownWriter) {
                isEnded = true
              }else{
                cursorUpdated = false //invalidate flag
                writeLogRecord(logRecord)
                persistStepCursor()
                storage.commit()
              }
            } else {
              if (buffer.get(buffer.size() - 1) == ShutdownWriter) {
                buffer.remove(buffer.size() - 1)
                isEnded = true
              }
              cursorUpdated = false // invalidate flag
              batchWrite(buffer)
              persistStepCursor()
              //println(s"writing ${buffer.size} logs at a time")
              storage.commit()
              buffer.clear()
            }
            addOutput(CursorUpdate(persistedStepCursor))
            //if(storage.name == "exampleJob-0f02-0") System.out.println("buffer size = "+logRecordQueue.size())
          }
          logRecordQueue.clear()
          storage.release()
          addOutput(ShutdownOutput)
        } catch {
          case e: Throwable =>
            e.printStackTrace()
        }
      }
    })

  private def batchWrite(buffer: util.ArrayList[LogRecord]): Unit = {
    buffer.toArray(Array[LogRecord]()).foreach(x => writeLogRecord(x))
  }

  private def writeLogRecord(record: LogRecord): Unit = {
    record match {
      case clr: ControlRecord =>
        storage.write(clr)
      case cursor: DPCursor =>
        persistedStepCursor = cursor.idx
        storage.write(cursor)
      case record: ChannelOrder =>
        storage.write(record)
      case UpdateStepCursor(cursor) =>
        //only write the last step cursor of batched log entries
        if(cursor > persistedStepCursor){
          cursorUpdated = true
          persistedStepCursor = cursor
        }
      case timer:TimerStart =>
        storage.write(timer)
      case window:WindowStart =>
        storage.write(window)
      case TruncateLog =>
        storage.truncateLog()
      case ShutdownWriter =>
      //skip
    }
  }

  private def persistStepCursor(): Unit ={
    if(cursorUpdated){
      storage.write(UpdateStepCursor(persistedStepCursor))
    }
  }

}
