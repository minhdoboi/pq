package priv.pq.reader

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.InputFile
import org.apache.parquet.schema.MessageType
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.Try

object ParquetSchemaReader extends SchemaReader {

  def dumpSchema(file: InputFile, conf: Configuration) : Unit = {
    println(readSchemaMessageType(file).toString)
  }
  def readSchemaMessageType(file: InputFile): MessageType = {
    ParquetFileReader.open(file).getFooter.getFileMetaData.getSchema
  }

  def readSchema(file: InputFile, conf: Configuration): Schema = {
    val schema = ParquetFileReader.open(file).getFooter.getFileMetaData.getSchema
    new AvroSchemaConverter(conf).convert(schema)
  }

  override def subsetSchema(schema: Schema, trees: List[Tree], namespace: String): Try[Schema] = {
    super.subsetSchema(schema, trees, namespace).map(fixSchema)
  }

  // hack for list, removing element levels as it is added later
  def fixSchema(schema: Schema): Schema = {
    fixSchema(schema, "")
  }

  def fixSchema(schema: Schema, namespace: String = ""): Schema = {
    schema.getType match {
      case Schema.Type.ARRAY ⇒
        val subSchema = if (schema.getElementType.getName == "list") {
          schema.getElementType.getField("element").schema()
        } else {
          schema.getElementType
        }
        Schema.createArray(fixSchema(subSchema, namespace))
      case Schema.Type.UNION ⇒
        val types = schema.getTypes.asScala.map(t => fixSchema(t, namespace)).toSeq
        Schema.createUnion(types: _*)
      case Schema.Type.RECORD ⇒
        val fields = schema.getFields.asScala.map { field ⇒
          new Schema.Field(field.name(), fixSchema(field.schema(), namespace  + "." + schema.getName), field.doc(), field.defaultVal())
        }
        Schema.createRecord(schema.getName, schema.getDoc, namespace, schema.isError, fields.asJava)
      case _ ⇒ schema
    }
  }
}
