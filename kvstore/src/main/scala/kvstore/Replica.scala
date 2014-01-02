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

  var persists = Map.empty[Long, (ActorRef, Set[Persist], Set[ActorRef])]

  val persistenceActor = context.actorOf(persistenceProps)

  context.system.scheduler.schedule(0 milliseconds, 100 milliseconds) {
    persists foreach {
      case (id, (_, persist, _)) => persist.foreach { p =>
        println("PersistRetry :: Sending Persist: " + p)
        persistenceActor ! p 
      }
    }
  }

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => 
      println("Leader :: Got JoinedPrimary")
      context.become(leader)
    case JoinedSecondary =>
      println("Replica :: Got JoinedSecondary")
      context.become(replica(0L))
  }

  /* TODO Behavior for  the leader role. */
  def leader: Receive = {
    case r @ Replicas(newReps) =>
      println("Leader :: Got Replicas: " + r)
      val existingReps = secondaries.keys.toSet
      val addedReps = newReps -- existingReps
      val removedReps = existingReps -- newReps
      println("\tAdded count: " + addedReps.size)
      println("\tRemvd count: " + removedReps.size)
      if (addedReps.nonEmpty) {
        addedReps.filter(addedRep => addedRep != self).foreach { addedRep =>
          // TODO: The replicator needs to be told about all KVPs at this point,
          // TODO: so that it's state is consistent with the rest of the system.
          secondaries += (addedRep -> context.actorOf(Replicator.props(addedRep)))
        }
      }
      if (removedReps.nonEmpty) {
        // TODO: The removed replicators must be terminated
        removedReps.foreach { removedRep => secondaries = secondaries - removedRep }
      }
      println("Leader :: After add/remove. secondaries: " + secondaries)
      replicators = secondaries.values.toSet

    case i @ Insert(key, value, id) =>
      println("Leader :: Got Insert: " + i)
      kv += key -> value
      val p = Persist(key, Some(value), id)
      println("Leader :: Persisting: " + p)
      persist(id, p, sender)
      val r = Replicate(key, Some(value), id)
      println("Leader :: Replicating: " + r)
      replicate(id, Replicate(key, Some(value), id), replicators, sender)

    case rem @ Remove(key, id) =>
      println("Leader :: Got Remove: " + rem)
      kv = kv - key
      val p = Persist(key, None, id)
      println("Leader :: Persisting: " + p)
      persist(id, p, sender)
      val r = Replicate(key, None, id)
      println("Leader :: Replicating: " + r)
      replicate(id, r, replicators, sender)

    case g @ Get(key, id) =>
      println("Leader :: Got Get: " + g)
      sender ! GetResult(key, kv.get(key), id)

    case p @ Persisted(key, id) =>
      println("Leader :: Got Persisted: " + p)
      persists.get(id).foreach {
        case (originalSender, _, reps) =>
          // If the ID is present, and the replicator set is empty (i.e. replication is complete), then we are good to ack.
          // There is only ever one entry in the persistence set, so don't bother removing the persistence item, just remove
          // the entire entry from the map.
          if (reps.isEmpty) {
            persists -= id
            originalSender ! OperationAck(id)
          } else {
            persists += (id -> (originalSender, Set.empty[Persist], reps))
          }
        case _ =>
      }

    case r @ Replicated(key, id) =>
      println("Leader :: Got Replicated: " + r)
      persists.get(id).foreach {
        case (s, ps, replicatorActors) =>
          val replicatorsStillWaitingFor = replicatorActors - sender
          if (replicatorsStillWaitingFor.isEmpty && ps.isEmpty) {
            // Not waiting for any replicators or persistence, so ack.
            persists -= id
            s ! OperationAck(id)
          } else {
            persists += id -> (s, ps, replicatorsStillWaitingFor)
          }
      }
  }

  /* TODO Behavior for the replica role. */
  def replica(expectedSeq: Long): Receive = LoggingReceive {
    case s @ Snapshot(_, _, seq) if seq > expectedSeq =>
      println("Replica :: Got Snapshot [greater than expectedSeq - ignoring]: " + s)
    case s @ Snapshot(key, _, seq) if seq < expectedSeq =>
      println("Replica :: Got Snapshot [less than expectedSeq]: " + s)
      sender ! SnapshotAck(key, seq)
      context.become(replica(getNextSeq(expectedSeq, seq)))
    case s @ Snapshot(key, Some(value), seq) =>
      println("Replica :: Got Snapshot: " + s)
      kv += key -> value
      context.become(replica(getNextSeq(expectedSeq, seq)))
      val p = Persist(key, Some(value), seq)
      println("Replica :: Persisting: " + p)
      persist(seq, p, sender)
    case s @ Snapshot(key, None, seq) =>
      println("Replica :: Got Snapshot: " + s)
      kv -= key
      context.become(replica(getNextSeq(expectedSeq, seq)))
      val p = Persist(key, None, seq)
      println("Replica :: Persisting: " + p)
      persist(seq, p, sender)
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case p @ Persisted(key, id) =>
      println("Replica :: Got Persisted: " + p)
      persists.get(id).foreach {
        case (originalSender, _, _) =>
          // Update the map to clear the persist.
          // replica nodes don't do replication.
          persists -= id
          val s = SnapshotAck(key, id)
          println("Replica :: Sending SnapshotAck: " + s)
          originalSender ! s
      }
    case x => println("Replica :: Got unknown message: " + x)
  }

  def replicate(id: Long, replicate: Replicate, replicators: Set[ActorRef], s: ActorRef) = {

    // There has to be a nice way of getting a tuple from a map, and then selecting an element for that tuple, 
    // while providing a default for the *element* if the tuple is not present in the map?
    val persist = if (persists.contains(id)) persists.get(id).map(_._2).head else Set.empty[Persist]

    persists += (id -> (s, persist, replicators))
    replicators.foreach { replicator =>
      replicator ! replicate
    }

    // If complete replication is not finished in a second,
    // then fail.
    context.system.scheduler.scheduleOnce(1 second) {
      if (persists.contains(id)) {
        //persists -= id
        s ! OperationFailed(id)
      }
    }
  }

  def persist(id: Long, persist: Persist, s: ActorRef) {

    // There has to be a nice way of getting a tuple from a map, and then selecting an element for that tuple, 
    // while providing a default for the *element* if the tuple is not present in the map?
    val replicators = if (persists.contains(id)) persists.get(id).map(_._3).head else Set.empty[ActorRef]

    persists += (id -> (s, Set(persist), replicators))
    persistenceActor ! persist

    // If nothing is heard from the persistenceActor within
    // a second, then fail.
    context.system.scheduler.scheduleOnce(1 second) {
      if (persists.contains(id)) {
        persists -= id
        // The secondaries actually aren't meant to send anything, but it doesn't hurt... ahem.
        s ! OperationFailed(id)
      }
    }
  }
}
