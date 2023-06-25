package com.wixpress.dst.greyhound.core.compression

import org.apache.commons.compress.utils.IOUtils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import scala.util.Try

object GzipCompression {
  def compress(input: Array[Byte]): Array[Byte] = {
    val bos        = new ByteArrayOutputStream(input.length)
    val gzip       = new GZIPOutputStream(bos)
    gzip.write(input)
    gzip.close()
    val compressed = bos.toByteArray
    bos.close()
    compressed
  }

  def decompress(compressed: Array[Byte]): Option[Array[Byte]] = {
    val byteStream = new ByteArrayInputStream(compressed)
    Try(new GZIPInputStream(byteStream))
      .flatMap(gzipStream =>
        Try {
          val result = IOUtils.toByteArray(gzipStream)
          gzipStream.close()
          byteStream.close()
          result
        }.recover {
          case e: Throwable =>
            Try(gzipStream.close())
            Try(byteStream.close())
            e.printStackTrace()
            throw e
        }
      )
      .toOption
  }
}
