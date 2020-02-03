package priv.pq.reader

import java.io.File

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.fs.{AvroFSInput, FileContext, Path}
import org.apache.parquet.io.InputFile

import scala.util.Try

class AvroReader(inputFile : InputFile) extends Reader[GenericRecord] {

  val datum = new GenericDatumReader[GenericRecord]()
  val input = new AvroFSInput(FileContext.getFileContext(), new Path(inputFile.toString))
  val reader = DataFileReader.openReader(input, datum)

  def read(): Option[GenericRecord] = {
    Try(reader.next()).toOption
  }

  def close(): Unit = {
    reader.close()
  }
}
