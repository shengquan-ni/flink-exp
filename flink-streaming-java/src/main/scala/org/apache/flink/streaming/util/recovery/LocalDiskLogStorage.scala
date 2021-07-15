package org.apache.flink.streaming.util.recovery

import java.io.{DataInputStream, DataOutputStream}
import java.nio.file.{Files, Paths, StandardOpenOption}

object LocalDiskLogStorage{
}


class LocalDiskLogStorage(name:String) extends FileLogStorage(name) {

  private lazy val path = Paths.get(s"./logs/$name.log")

  override def getInputStream: DataInputStream = new DataInputStream(Files.newInputStream(path))

  override def getOutputStream: DataOutputStream = {
    new DataOutputStream(
      Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    )
  }

  override def fileExists: Boolean = Files.exists(path)

  override def createDirectories(): Unit = Files.createDirectories(path.getParent)

  override def deleteFile(): Unit = Files.delete(path)
}
