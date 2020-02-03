package priv.pq

import priv.pq.reader.Tree

case class BootParams(
                       path: String                = "",
                       fields: List[List[String]]  = Nil,
                       readLimit: Option[Int]      = None,
                       where: Option[String]       = None,
                       distinct : Boolean          = false,
                       summary : Boolean           = false,
                       dumpSchema : Boolean        = false,
                       format : Format.Value = Format.Parquet
) {
  def fieldTrees = Tree.fromList(fields)
}

object Format extends Enumeration {
  val Avro = Value("avro")
  val Parquet = Value("parquet")
}


object BootParams {
  import scopt._

  def parse(args: Array[String]): Option[BootParams] = parser.parse(args, BootParams())

  val parser = new OptionParser[BootParams]("pq") {

    opt[Seq[String]]('f', "fields")
      .optional()
      .text("what to display")
      .action((x, p) ⇒ p.copy(fields = x.toList.map(_.split("\\.").toList)))

    opt[Int]( "read-limit")
      .optional()
      .text("limit the data read in case the volume is too big")
      .action((x, p) ⇒ p.copy(readLimit = Some(x)))

    opt[String]('w', "where")
      .optional()
      .text("a filter expression")
      .action((x, p) ⇒ p.copy(where = Some(x)))

    opt[Unit]("distinct")
      .optional()
      .action((_, p) ⇒ p.copy(distinct = true))

    opt[Unit]("summary")
      .optional()
      .action((_, p) ⇒ p.copy(summary = true))

    opt[Unit]("dumpSchema")
      .optional()
      .action((_, p) ⇒ p.copy(dumpSchema = true))

    opt[String]('t', "format")
      .optional()
      .text("format (default=" + BootParams().format+")")
      .action((x, p) ⇒ p.copy(format = Format.withName(x)))

    arg[String]("path")
      .required()
      .action((x, p) ⇒ p.copy(path = x))

  }
}