package priv.pq

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroReadSupport, AvroSchemaConverter, AvroWriteSupport}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.slf4j.LoggerFactory
import priv.pq.reader.{AvroReader, AvroSchemaReader, ParquetReader, ParquetSchemaReader, SchemaOps, SchemaReader}

import scala.util.Using

object Main extends App {
  val params = BootParams.parse(args).get
  val runner = new Runner(params)

  if (params.dumpSchema) {
    runner.dumpSchema()
  } else {
    runner.explore().get
  }
}

class Runner(params : BootParams) {

  private lazy val logger = LoggerFactory.getLogger(getClass)
  val conf = new Configuration()
  val inputFile = HadoopInputFile.fromPath(new Path(params.path), conf)
  val schemaReader : SchemaReader = params.format match {
    case Format.Parquet => ParquetSchemaReader
    case Format.Avro => AvroSchemaReader
  }

  val schema = schemaReader.readSchema(inputFile, conf)
  val writeOldStructure = !schema.hasElementField

  def dumpSchema() = {
    schemaReader.dumpSchema(inputFile, conf)
  }

  def explore() = {
    logger.debug("Explore " + params + ", " + params.fieldTrees)

    conf.set(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, writeOldStructure.toString)
    if (params.fieldTrees.nonEmpty) {
      val subset = schemaReader.subsetSchema(schema, params.fieldTrees).get

      logger.debug(new AvroSchemaConverter(conf).convert(subset).toString)
      logger.debug("project on " + subset.toString(true))
      AvroReadSupport.setRequestedProjection(conf, subset)
    }
    val reader = params.format match {
      case Format.Parquet => new ParquetReader(inputFile)
      case Format.Avro => new AvroReader(inputFile)
    }

    Using(reader) { reader =>
      val data = applyDistinct(
        FilterExpr
          .applyFilter(applyReadLimit(reader.lazyList).filter(FilterExpr.hasValue), params))
        .map { record ⇒ applyRemoveJsonElement(record.toString) }

      if (params.summary) {
        logger.info("Found " + data.size + " values")
        val limit = 100
        val lineLimit = 150
        data.take(limit) foreach { line =>
          println(if (line.length > lineLimit) line.substring(0, lineLimit) + "... ("+ (line.size - lineLimit) +" more chars)" else line)
        }
        if (data.size > limit) {
          println("... ("+ (data.size - limit) + ") more values)")
        }
      } else {
        data foreach println
      }
    }
  }

  def applyRemoveJsonElement(json : String) : String = {
    if (writeOldStructure) json else Formatter.removeJsonElements(json)
  }

  def applyDistinct(l : Seq[GenericRecord]): Seq[GenericRecord] = {
    if (params.distinct) l.distinct else l
  }

  def applyReadLimit[A](l : Seq[A]): Seq[A] = {
    params.readLimit.fold(l)(limit => l.take(limit))
  }

}