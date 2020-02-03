package priv.pq.reader

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.io.InputFile

class ParquetReader(inputFile : InputFile) extends Reader[GenericRecord] {
  val parquetReader = AvroParquetReader.builder[GenericRecord](inputFile)
    .withDataModel(GenericData.get())
    .build()

  def read(): Option[GenericRecord] = {
    Option(parquetReader.read())
  }

  def close(): Unit = {
    parquetReader.close()
  }
}
