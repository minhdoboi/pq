package priv.pq

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.slf4j.LoggerFactory

import scala.util.parsing.combinator.JavaTokenParsers
import scala.jdk.CollectionConverters._

sealed trait FilterExpr
case class UnaryOperator(op: String, expr: FilterExpr) extends FilterExpr
case class Operator(op: String, expr1: FilterExpr, expr2: FilterExpr) extends FilterExpr
case class Sym(name: String) extends FilterExpr
case class Lit(value: Any) extends FilterExpr

class FilterExprParser extends JavaTokenParsers {
  def not: Parser[UnaryOperator] = "!" ~> expr ^^ { x ⇒ UnaryOperator("!", x) }
  def exists: Parser[UnaryOperator] = "exists" ~> ident ^^ { x ⇒ UnaryOperator("exists", Sym(x)) }
  def notExists: Parser[UnaryOperator] = "!!" ~> ident ^^ { x ⇒ UnaryOperator("!!", Sym(x)) }
  def eq: Parser[Operator] = sym ~ "=" ~ lit ^^ { case x ~ op ~ y ⇒ Operator(op, x, y) }
  def and: Parser[Operator] = group ~ "&&" ~ group ^^ { case x ~ op ~ y ⇒ Operator(op, x, y) }
  def or: Parser[Operator] = group ~ "||" ~ group ^^ { case x ~ op ~ y ⇒ Operator(op, x, y) }
  def lit: Parser[Lit] = {
    wholeNumber.map(x => Lit(x.toInt)) |
      decimalNumber.map( x ⇒ Lit(x.toDouble) ) |
      stringLiteral.map(x => Lit(x.substring(1, x.length - 1)))
  }
  def sym : Parser[Sym] = ident.map(Sym)
  def group : Parser[FilterExpr] = "(" ~> expr <~ ")"
  def expr: Parser[FilterExpr] = and | or | group | eq | not | exists | notExists | lit | sym
}

object FilterExpr {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def parse(expr: String): FilterExpr = {
    val parser = new FilterExprParser
    val result = parser.parseAll(parser.expr, expr)
    result.get
  }

  def hasValue(value : Any) : Boolean = {
    value match {
      case r : GenericRecord =>
          val s = r.getSchema
          s.getFields.asScala
            .iterator
            .exists(f => Option(r.get(f.name())).exists(hasValue))
      case arr : GenericData.Array[_] =>
        arr.iterator().asScala.exists(hasValue)
      case null => false
      case _ => true
    }
  }

  def applyFilter(seq: Seq[GenericRecord], params: BootParams) : Seq[GenericRecord] = {
    params.where.map { FilterExpr.parse } match {
      case None       ⇒ seq
      case Some(expr) ⇒
        logger.debug("expr " + expr)
        seq.filter { value ⇒ FilterExpr.evaluate(expr, value) == true }
    }
  }

  def evaluate(expr: FilterExpr, value: Any): Any = {
    expr match {
      case UnaryOperator("!", Sym(name))      ⇒ ! find(name, value).contains(true)
      case UnaryOperator("exists", Sym(name)) ⇒ find(name, value).nonEmpty
      case UnaryOperator("!!", Sym(name))     ⇒ find(name, value).isEmpty
      case Sym(name)                          ⇒ find(name, value).map(coerce)
      case Lit(lit)                           ⇒ lit
      case Operator("=", x, y)                ⇒
        val a = evaluate(x, value)
        val b = evaluate(y, value)
        a match {
          case a: Seq[Any] => a.contains(b)
          case a => a == b
        }
      case Operator("&&", x, y)               ⇒ evaluate(x, value) ==true && evaluate(y, value)== true
      case Operator("||", x, y)               ⇒ evaluate(x, value) ==true || evaluate(y, value)== true
      case _                                  ⇒ false
    }
  }

  val coerce : Any => Any = {
    case utf8: Utf8 => utf8.toString
    case x => x
  }

  def find(name : String, value : Any) : Seq[Any] = {
    val res = value match {
      case r : GenericRecord => 
        Option(r.get(name)) match {
          case Some(v) => Seq(v)
          case None =>
            r.getSchema
              .getFields.asScala
              .iterator
              .flatMap(f => Option(r.get(f.name())))
              .flatMap( v => find(name, v) )
              .filter(_ !=null)
              .toSeq
        }
      case arr : java.util.AbstractCollection[_] =>
        arr.iterator().asScala.flatMap(v => find(name, v)).filter(_ !=null).toSeq
      case _ => Seq.empty
    }
    logger.trace("Find " + name + " in " + value + ","+ value.getClass + "->" +res)
    res
  }
}
