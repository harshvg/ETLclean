import org.scalatest.FunSuite

import scala.collection.mutable

class Query3ETLTest extends FunSuite{

  test("Query3ETL.Censor"){

    // test rot13 function
    assert(Query3ETL.rot13("15619ppgrfg")==="15619cctest")
    assert(Query3ETL.rot13("avttre")==="nigger")

    // test censor functions
    assert(Query3ETL.censorData("nigger is fuck ass cold")=== "n****r is f**k a*s cold")
    assert(Query3ETL.censorData("n-i-g ger is fuck ass cold")=== "n-i-g ger is f**k a*s cold")
    assert(Query3ETL.censorData("n/i/g/g/e/r is fuck ass cold")=== "n/i/g/g/e/r is f**k a*s cold")

    // test impact score
    assert(Query3ETL.calImpactSccore("I hate uuuuioiu harsha",1,1,1)===9)
    assert(Query3ETL.calImpactSccore("I I I I",2,20,30)===0)

    //test Topic words map
    assert(Query3ETL.generateTopicWords("harsha is harsha abcf hero")=== new mutable.HashMap(("harsha",1)))


  }

}
