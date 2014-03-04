package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.concurrent.duration._
import scala.language.postfixOps
import kvstore.Replicator.{SnapshotAck, Snapshot}
import kvstore.Persistence.{Persisted, Persist}
import kvstore.Replica._
import kvstore.Replica.Remove
import kvstore.Persistence.Persist
import kvstore.Replicator.Snapshot
import kvstore.Replica.Get
import kvstore.Replicator.SnapshotAck
import kvstore.Replica.GetResult
import kvstore.Replica.Insert

object Replica {
  val PersistDuration: Duration = 1 second

  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(subContracted(context.actorOf(Props(new PrimaryReplica(persistenceProps)))))
    case JoinedSecondary => context.become(subContracted(context.actorOf(Props(new SecondaryReplica(persistenceProps)))))
  }

  def subContracted(subContractor: ActorRef): Receive = {
    case m => subContractor forward m
  }
}

class PrimaryReplica(persistenceProps: Props) extends Actor {
  val persistenceActor = context.actorOf(persistenceProps)
  context.become(receive())

  def receive: Receive = {
    case _ =>
  }

  def receive(kv: Map[String, String] = Map.empty[String, String]): Receive = {
    case Insert(key, value, id) =>
      val persist = Persist(key, Some(value), id)
      val persister = context.actorOf(Props(new Persister(persist, persistenceActor)))
      context.become(receivePersist(kv, sender, persister, persist))
    case Remove(key, id) =>
      val persist = Persist(key, None, id)
      val persister = context.actorOf(Props(new Persister(persist, persistenceActor)))
      context.become(receivePersist(kv, sender, persister, persist))
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case _ =>
  }

  def receivePersist(kv: Map[String, String], client: ActorRef, persister: ActorRef, persist: Persist): Receive = {
    case Persister.PersistComplete(key, seq) =>
      val newKv =
        if (persist.valueOption.isDefined)
          kv + (persist.key -> persist.valueOption.get)
        else
          kv - persist.key
      context.become(receive(newKv))
      client ! OperationAck(seq)
    case Persister.PersistFailed(key, seq) =>
      context.become(receive(kv))
      client ! OperationFailed(seq)
  }
}

class SecondaryReplica(persistenceProps: Props) extends Actor {
  val persistenceActor = context.actorOf(persistenceProps)
  context.become(receive())

  var _expectedSeqCounter = 0L
  def nextExpectedSeq = {
    val ret = _expectedSeqCounter
    _expectedSeqCounter += 1
    ret
  }

  def receive: Receive = {
    case _ =>
  }

  def receive(kv: Map[String, String] = Map.empty[String, String], expectedSeq: Long = nextExpectedSeq): Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case s @ Snapshot(key, valueOption, seq) if seq == expectedSeq =>
      val persist = Persist(key, valueOption, seq)
      val persister = context.actorOf(Props(new Persister(persist, persistenceActor)))
      context.become(receivePersist(kv, sender, persister, persist))
    case Snapshot(key, _, seq) if seq < expectedSeq => sender ! SnapshotAck(key, seq)
    case _ =>
  }

  def receivePersist(kv: Map[String, String], client: ActorRef, persister: ActorRef, persist: Persist): Receive = {
    case Persister.PersistComplete(key, id) =>
      context.stop(persister)

      val newKv =
        if (persist.valueOption.isDefined)
          kv + (persist.key -> persist.valueOption.get)
        else
          kv - persist.key
      context.become(receive(newKv))
      client ! SnapshotAck(key, id)
    case Persister.PersistFailed =>
      context.stop(persister)
      context.become(receive(kv))
  }
}
