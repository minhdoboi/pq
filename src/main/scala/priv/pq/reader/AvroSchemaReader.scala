package priv.pq.reader

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.io.InputFile

import scala.util.Using

object AvroSchemaReader extends SchemaReader {

  def readSchema(file: InputFile, conf: Configuration): Schema = {
    Using(new AvroReader(file))(_.reader.getSchema).get
  }

  def dumpSchema(file: InputFile, conf: Configuration): Unit = {
    val schema = readSchema(file, conf)
    println(schema)
  }

}