package kvstore

import kvstore.Persistence.{Persisted, Persist}
import akka.actor.{ReceiveTimeout, Props, Actor, ActorRef}
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.event.LoggingReceive

object Persister {
  case class PersistFailed(key: String, id: Long)
  case class PersistComplete(key: String, id: Long)
}

class Persister(persist: Persist, persistenceActor: ActorRef, timeoutPeriod: Duration = 1 seconds) extends Actor {
  import Persister._

  context.setReceiveTimeout(timeoutPeriod)
  val retryer = context.actorOf(Props(new Retryer(persistenceActor, persist)))

  override def receive: Actor.Receive = LoggingReceive {
    case Persisted(key, id) =>
      context.setReceiveTimeout(Duration.Undefined)
      context.stop(retryer)
      context.parent ! PersistComplete(key, id)
      context.stop(self)
    case ReceiveTimeout =>
      context.setReceiveTimeout(Duration.Undefined)
      context.stop(retryer)
      context.parent ! PersistFailed(persist.key, persist.id)
      context.stop(self)
  }
}