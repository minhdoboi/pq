package priv.pq

import org.scalatest.{FunSuite, Matchers}

class FormatterTest extends FunSuite with Matchers {

  test("formatter should remove json elements") {
    val result = Formatter.removeJsonElements(
      """
        |{
        |  "obj":{
        |   "element":{
        |     "tata":"aa",
        |     "toto":"bb",
        |     "element": {
        |      "titi": "c"
        |     }
        |   }
        |  },
        |  "arr":[ {"element": {"aa": { "dd" : 1}}}]
        |}""".stripMargin)
    result shouldBe """{"obj":{"tata":"aa","toto":"bb","titi":"c"},"arr":[{"aa":{"dd":1}}]}"""
  }

}
