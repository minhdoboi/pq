package priv.pq

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecordBuilder
import org.scalatest.{FunSuite, Matchers}

import scala.jdk.CollectionConverters._

class FilterExprTest extends FunSuite with Matchers {

  test("should parse exists") {
    FilterExpr.parse("exists toto") shouldBe UnaryOperator("exists", Sym("toto"))
  }

  test("should parse an id") {
    FilterExpr.parse("toto") shouldBe Sym("toto")
  }

  test("should parse a lit") {
    FilterExpr.parse("1") shouldBe Lit(1)
  }

  test("should parse =") {
    FilterExpr.parse("toto = 1") shouldBe Operator("=", Sym("toto"), Lit(1))
  }

  test("should parse ()") {
    FilterExpr.parse("(toto = 1)") shouldBe Operator("=", Sym("toto"), Lit(1))
    FilterExpr.parse("! (toto = 1)") shouldBe UnaryOperator("!", Operator("=", Sym("toto"), Lit(1)))
  }

  test("should parse &&") {
    FilterExpr.parse("(toto=1) && (tata=2)") shouldBe Operator("&&",
      Operator("=", Sym("toto"), Lit(1)),
      Operator("=", Sym("tata"), Lit(2))
    )
  }

  test("should parse ||") {
    FilterExpr.parse("(toto=1) || (tata=2)") shouldBe Operator("||",
      Operator("=", Sym("toto"), Lit(1)),
      Operator("=", Sym("tata"), Lit(2))
    )
  }

  def createRecord(name : String, fields : Schema.Field*) = {
    val schema = Schema.createRecord("root", "doc", "ns", false)
    schema.setFields(fields.asJava)
    schema
  }

  val childSchema = createRecord("child",
    new Schema.Field("toto", Schema.create(Schema.Type.STRING), null, "0")
  )
  val rootSchema = createRecord("root",
    new Schema.Field("id", Schema.create(Schema.Type.STRING), null, "0"),
    new Schema.Field("arr", Schema.createArray(childSchema), null, Nil.asJava))

  test("should find a value at root level") {
    val expr =  FilterExpr.parse("id=\"2\"")

    val notFoundRecord = new GenericRecordBuilder(rootSchema).set("id", "1").build()
    FilterExpr.evaluate(expr, notFoundRecord) shouldBe false

    val record = new GenericRecordBuilder(rootSchema).set("id", "2").build()
    FilterExpr.evaluate(expr, record) shouldBe true
  }

  test("should find a value in a collection") {
    val expr = FilterExpr.parse("toto=3")

    val notFoundRecord = new GenericRecordBuilder(rootSchema)
      .set("id", "aze")
      .set("arr", List(new GenericRecordBuilder(childSchema).set("toto", 2).build()).asJava).build()
    FilterExpr.evaluate(expr, notFoundRecord) shouldBe false

    val record = new GenericRecordBuilder(rootSchema)
      .set("id", "aze")
      .set("arr", List(
        new GenericRecordBuilder(childSchema).set("toto", 1).build(),
        new GenericRecordBuilder(childSchema).set("toto", 3).build()).asJavaCollection).build()
    FilterExpr.evaluate(expr, record) shouldBe true
  }

}
