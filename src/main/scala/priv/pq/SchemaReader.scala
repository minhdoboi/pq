package priv.pq

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.InputFile
import org.apache.parquet.schema.MessageType
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

/**
  * Utils to read schema, and extract a subset of it
  */

object Tree {

  def fromList(value: List[List[String]]): List[Tree] = {
    value.groupBy(_.headOption).toList.flatMap {
      case (None, children)         ⇒ children.flatMap { c ⇒ if (c.isEmpty) Nil else fromList(List(c)) }
      case (Some(common), children) ⇒ List(Tree(common, fromList(children.map(_.tail))))
    }
  }
}

case class Tree(name: String, tree: List[Tree])

object SchemaReader {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def readSchemaMessageType(file: InputFile, conf: Configuration): MessageType = {
    ParquetFileReader.open(file).getFooter.getFileMetaData.getSchema
  }

  def readSchema(file: InputFile, conf: Configuration): Schema = {
    val schema = ParquetFileReader.open(file).getFooter.getFileMetaData.getSchema
    new AvroSchemaConverter(conf).convert(schema)
  }

  implicit class SchemaOps(schema: Schema) {

    def findField(name: String): Try[Schema.Field] = {
      Option(schema.getField(name)) match {
        case Some(field) ⇒ Success(field)
        case None ⇒
          val schemaFields = listFields
          Failure(new Exception("Field " + name + " not found in " + schema.getName + ". It has :\n" + schemaFields.mkString("\n")) with NoStackTrace)
      }
    }

    def hasElementField : Boolean = listAllFields.exists(_.name() == "element")

    def listFields: Seq[String] = schema.getFields.asScala.map(_.name()).sorted.toSeq

    def listAllFields : Seq[Schema.Field] = {
      schema.getType match {
        case Schema.Type.RECORD =>
          schema.getFields.asScala.flatMap( field => field +: field.schema().listAllFields).toSeq
        case Schema.Type.ARRAY =>
          schema.getElementType.listAllFields
        case Schema.Type.UNION =>
          schema.getTypes.asScala.flatMap(_.listAllFields).toSeq
        case _ => Nil
      }
    }
  }

  def subsetSchema(schema: Schema, trees: List[Tree], namespace: String = ""): Try[Schema] = {
    schema.getType match {
      case Schema.Type.ARRAY ⇒
        subsetSchema(schema.getElementType, trees, namespace).map { elementSchema ⇒
          logger.debug("Array " + schema.getName + ", element " + elementSchema.getName)
          Schema.createArray(elementSchema)
        }
      case Schema.Type.UNION ⇒
        Utils.traverseTry(schema.getTypes.asScala.toSeq) { t ⇒
          if (t.getType == Schema.Type.NULL) Success(t)
          else subsetSchema(t, trees, namespace)
        }.map { subSchemas ⇒
          logger.debug("Union " + schema.getName + ", element " + subSchemas.map(_.getName).mkString(","))
          Schema.createUnion(subSchemas: _*)
        }
      case Schema.Type.RECORD ⇒
        val fields = if (schema.getName == "list") {
          Seq((schema.findField("element"), trees))
        } else {
          trees.map { tree ⇒ (schema.findField(tree.name), tree.tree) }
        }
        Utils.traverseTry(fields) {
          case (field, nexts) ⇒
            field.flatMap { field ⇒
              if (nexts.isEmpty) {
                Success((field, field.schema()))
              } else {
                subsetSchema(field.schema(), nexts, namespace + "." + schema.getName).map { schema ⇒
                  (field, schema)
                }
              }
            }
        }.map { childSchemas ⇒
          Schema.createRecord(schema.getName, schema.getDoc, namespace, schema.isError,
            childSchemas.map {
              case (field, childSchema) ⇒
                new Schema.Field(field.name(), childSchema, field.doc(), field.defaultVal())
            }.asJava)
        }
      case _ ⇒ throw new Exception("Not managed " + schema.getType)
    }
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
