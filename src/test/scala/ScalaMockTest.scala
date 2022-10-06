import com.format.Greetings
import com.format.Greetings.Formatter
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ScalaMockTest extends AnyFunSuite with MockFactory  {

  test("class Object mock test"){
    val mockFormatter = mock[Formatter]

    //mockFormatter.format.("Mr Bond").returning("Ah, Mr Bond. I've been expecting you").once()

    Greetings.sayHello("Mr Bond", mockFormatter)

  }
}
