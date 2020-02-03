package priv.pq.reader

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.io.InputFile
import org.slf4j.LoggerFactory
import priv.pq.Utils

import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

trait SchemaReader {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def dumpSchema(file: InputFile, conf: Configuration): Unit

  def readSchema(file: InputFile, conf: Configuration): Schema

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
}


object Tree {

  def fromList(value: List[List[String]]): List[Tree] = {
    value.groupBy(_.headOption).toList.flatMap {
      case (None, children)         ⇒ children.flatMap { c ⇒ if (c.isEmpty) Nil else fromList(List(c)) }
      case (Some(common), children) ⇒ List(Tree(common, fromList(children.map(_.tail))))
    }
  }
}

case class Tree(name: String, tree: List[Tree])
