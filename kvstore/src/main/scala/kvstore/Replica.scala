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
import scala.language.postfixOps
import akka.event.LoggingReceive

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

  def getNextSeq(currentSeq: Long, justAckedSeq: Long) = if (currentSeq > (justAckedSeq + 1)) currentSeq else justAckedSeq + 1
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var persists = Map.empty[Long, (ActorRef, Persist)]

  val persistenceActor = context.actorOf(persistenceProps)

  context.system.scheduler.schedule(0 milliseconds, 100 milliseconds) {
    persists foreach {
      case (id, (_, persist)) => persistenceActor ! persist
    }
  }

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica(0L))
  }

  /* TODO Behavior for  the leader role. */
  def leader(replicationsInProgress: Map[Long, (ActorRef, Set[ActorRef])]): Receive = {
    case Replicas(newReps) =>
      // Not amazingly efficient...
      val existingReps = secondaries.keys.toSet
      val addedReps = newReps -- existingReps
      val removedReps = existingReps -- newReps
      if (addedReps.nonEmpty) {
        secondaries = secondaries ++
          (addedReps zip (addedReps map { _ =>
            // TODO: Does this replicator need to be started in some way?
            // TODO: The replicator needs to be told about all KVPs at this point,
            // TODO: so that it's state is consistent with the rest of the system.
            context.actorOf(Props[Replicator])
          }))
      }
      if (removedReps.nonEmpty) {
        // TODO: The removed replicators must be terminated
        removedReps.foreach { removedRep => secondaries = secondaries - removedRep }
      }
      replicators = secondaries.values.toSet
    case Insert(key, value, id) =>
      kv += key -> value
      persist(id, Persist(key, Some(value), id), sender)
    case Remove(key, id) =>
      kv = kv - key
      persist(id, Persist(key, None, id), sender)
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Persisted(key, id) =>
      persists.get(id).foreach {
        case (originalSender, _) =>
          persists -= id

          originalSender ! OperationAck(id)
      }
    case Replicated(key, id) =>
      replicationsInProgress.get(id).foreach {
        case (s, replicatorActors) =>
          val replicatorsStillWaitingFor = replicatorActors - sender
          if (replicatorsStillWaitingFor.isEmpty) {
            context.become(leader(replicationsInProgress - ))
            if (!persists.contains(id)) {
              s ! OperationAck(id)
            }
          } else {
            context.become(leader(replicationsInProgress + (id -> (s, replicatorsStillWaitingFor))))
          }
      }
  }

  /* TODO Behavior for the replica role. */
  def replica(expectedSeq: Long): Receive = LoggingReceive {
    case Snapshot(_, _, seq) if seq > expectedSeq => // Do nothing
    case Snapshot(key, _, seq) if seq < expectedSeq =>
      sender ! SnapshotAck(key, seq)
      context.become(replica(getNextSeq(expectedSeq, seq)))
    case Snapshot(key, Some(value), seq) =>
      kv += key -> value
      context.become(replica(getNextSeq(expectedSeq, seq)))
      persist(seq, Persist(key, Some(value), seq), sender)
    case Snapshot(key, None, seq) =>
      kv -= key
      context.become(replica(getNextSeq(expectedSeq, seq)))
      persist(seq, Persist(key, None, seq), sender)
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Persisted(key, id) =>
      persists.get(id).foreach {
        case (originalSender, _) =>
          persists -= id
          originalSender ! SnapshotAck(key, id)
      }
  }

  def persist(id: Long, persist: Persist, s: ActorRef) {
    persists += id -> (s, persist)
    persistenceActor ! persist

    // If nothing is heard from the persistenceActor within
    // a second, then fail.
    context.system.scheduler.scheduleOnce(1 second) {
      if (persists.contains(id)) {
        persists -= id
        s ! OperationFailed(id)
      }
    }
  }
}
