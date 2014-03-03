package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
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
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var _expectedSeqCounter = 0L
  def nextExpectedSeq = {
    val ret = _expectedSeqCounter
    _expectedSeqCounter += 1
    ret
  }
  
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader())
    case JoinedSecondary => context.become(replica())
  }

  def leader(kv: Map[String, String] = Map.empty[String, String]): Receive = {
    case Insert(key, value, id) =>
      context.become(leader(kv + (key -> value)))
      sender ! OperationAck(id)
    case Remove(key, id) =>
      context.become(leader(kv - key))
      sender ! OperationAck(id)
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case _ =>
  }

  /* TODO Behavior for the replica role. */
  def replica(kv: Map[String, String] = Map.empty[String, String], expectedSeq: Long = nextExpectedSeq): Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case s @ Snapshot(key, valueOption, seq) if seq == expectedSeq =>
      if (valueOption.isDefined)
        context.become(replica(kv + (key -> valueOption.get)))
      else
        context.become(replica(kv - key))
      sender ! SnapshotAck(key, seq)
    case Snapshot(key, _, seq) if seq < expectedSeq => sender ! SnapshotAck(key, seq)
    case _ =>
  }
}
