package priv.pq

import org.apache.avro.Schema

import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._

package object reader {

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
}
