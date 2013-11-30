package nodescala



import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be created") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }

  test("A Future should never be created") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("Now should return the result") {
    val always = Future.always(517)
    assert(always.now == 517)
  }

  test("Now should throw a NoSuchElementException") {
    val never = Future.never[Int]
    try {
      never.now
      assert(false)
    } catch {
      case t: NoSuchElementException => // ok!
    }
  }

  test("Any should return the result of the first completing Future") {
    val futures = (1 to 3).map { x => Future { Thread.sleep(x * 1000); x; } }
    val f = Future.any(futures.toList)
    try {
      val result = Await.result(f, 1500 milliseconds)
      assert(result == 1)
    } catch {
      case t: Exception => assert(false)
    }
  }

  test("Any should return the failure of the first failing Future") {
    class ShizzleException extends Exception

    val futures = (1 to 3).map { x => Future { Thread.sleep(x * 2000); throw new ShizzleException; } }
    val f = Future.any(futures.toList)
    try {
      val result = Await.result(f, 3 seconds)
      assert(false)
    } catch {
      case t: TimeoutException => assert(false)
      case t: ShizzleException =>
    }
  }

  test("All should return all values from a list of Futures") {
    val f = Future.all((1 to 3).map { x => Future(x) }.toList)
    try {
      val result = Await.result(f, 1 seconds)
      assert(result == List(1,2,3))
    } catch {
      case t: Exception => assert(false)
    }
  }

  test("All should fail if one of the child Futures fails") {
    class ShizzleException extends Exception
    val f = Future.all((1 to 3).map { x => Future { if (x != 3) x else throw new ShizzleException }}.toList)
    try {
      Await.result(f, 1 seconds)
      assert(false)
    } catch {
      case t: ShizzleException => assert(true)
    }
  }

  test("ContinueWith should return a new Future containing the result of cont") {
    val stringVal = "Who cares about the input"

    val newFuture = Future(5).continueWith(_ => stringVal)
    val result = Await.result(newFuture, 1 seconds)
    assert(result == stringVal)
  }

  test("ContinueWith should only invoke cont when the initial future completes") {
    val stringVal = "Who cares about the input"

    var contCalled = false
    val firstFuture = Future {
      Thread.sleep(1000)
      5
    }

    val cont = (a: Future[Int]) => {
      contCalled = true
      stringVal
    }

    val secondFuture = firstFuture.continueWith(cont)
    assert(!contCalled)

    Await.result(secondFuture, 1 seconds)
    assert(contCalled)
  }

  test("CancellationTokenSource should allow stopping the computation") {
    val cts = CancellationTokenSource()
    val ct = cts.cancellationToken
    val p = Promise[String]()

    async {
      while (ct.nonCancelled) {
        // do work
      }

      p.success("done")
    }

    cts.unsubscribe()
    assert(Await.result(p.future, 1 second) == "done")
  }

  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }

  test("Listener should serve the next request as a future") {
    val dummy = new DummyListener(8191, "/test")
    val subscription = dummy.start()

    def test(req: Request) {
      val f = dummy.nextRequest()
      dummy.emit(req)
      val (reqReturned, xchg) = Await.result(f, 1 second)

      assert(reqReturned == req)
    }

    test(immutable.Map("StrangeHeader" -> List("StrangeValue1")))
    test(immutable.Map("StrangeHeader" -> List("StrangeValue2")))

    subscription.unsubscribe()
  }

  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

}




