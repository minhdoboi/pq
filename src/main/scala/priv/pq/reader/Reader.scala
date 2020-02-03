package priv.pq.reader

trait Reader[A] extends AutoCloseable {
  def read(): Option[A]
  def close(): Unit

  def lazyList = LazyList
    .continually(read())
    .takeWhile(_.isDefined)
    .flatten
}
