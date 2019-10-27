package priv.pq

import scala.collection.BuildFrom
import scala.util.Try

object Utils {

  def traverseTry[A, B, M[X] <: IterableOnce[X]](values: M[A])(f: A ⇒ Try[B])(implicit bf: BuildFrom[M[A], B, M[B]]): Try[M[B]] = {
    values.iterator.foldLeft(Try(bf(values))) { (acc, elt) ⇒
      for {
        acc ← acc
        res ← f(elt)
      } yield acc += res
    }.map(_.result())
  }
}

trait Reader[A] extends AutoCloseable {
  def read(): Option[A]
  def close(): Unit

  def lazyList = LazyList
    .continually(read())
    .takeWhile(_.isDefined)
    .flatten
}