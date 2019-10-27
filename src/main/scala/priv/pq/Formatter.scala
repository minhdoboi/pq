package priv.pq

object Formatter {

  def removeJsonElements(str : String) : String = {
    val json = ujson.read(str)
    removeElements(json)
    ujson.write(json)
  }

  def removeElements(json : ujson.Value) : ujson.Value  = {
    json match{
      case a: ujson.Arr =>
        ujson.Arr(a.arr.map(removeElements))
      case o: ujson.Obj =>
        ujson.Obj(o.obj.flatMap{
          case ("element", v) =>
            o.obj.remove("element")
            val child = removeElements(v.obj)
            o.obj.addAll(child.obj)
            o.obj
          case (k, v ) => Seq(k -> removeElements(v))
        })
      case _ => json
    }
  }

}
