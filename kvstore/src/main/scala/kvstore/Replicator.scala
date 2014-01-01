package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  context.system.scheduler.schedule(0 milliseconds, 100 milliseconds) {
    acks foreach {
      case (seq, (_, Replicate(k, v, id))) => replica ! Snapshot(k, v, seq)
    }
  }

  def receive: Receive = {
    case r @ Replicate(key, value, id) =>
      println("Replicator :: Got Replicate: " + r)
      val seq = nextSeq
      val snapshot = Snapshot(key, value, seq)
      println("Replicator :: Sending Snapshot: " + snapshot)
      replica ! snapshot
      acks += seq -> (sender, r)
    case sa @ SnapshotAck(key, seq) =>
      println("Replicator :: Got SnapshotAck: " + sa)
      acks.get(seq).foreach {
        case (originalSender, replicate) =>
          acks -= seq
          val replicated = Replicated(replicate.key, replicate.id)
          println("Replicator :: Sending Replicated: " + replicated)
          originalSender ! replicated
      }
  }
}
