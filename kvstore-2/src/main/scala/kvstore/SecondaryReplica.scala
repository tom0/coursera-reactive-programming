package kvstore

import akka.actor.{ActorRef, Actor, Props}
import kvstore.Replicator.{Snapshot, SnapshotAck}
import kvstore.Persistence.Persist
import kvstore.Replica.{Get, GetResult}
import kvstore.Persister.{PersistFailed, PersistComplete}

class SecondaryReplica(persistenceProps: Props) extends Actor {
  val persistenceActor = context.actorOf(persistenceProps)

  var _expectedSeqCounter = 0L
  def nextExpectedSeq = {
    val ret = _expectedSeqCounter
    _expectedSeqCounter += 1
    ret
  }

  context.become(receive(expectedSeq = nextExpectedSeq))

  // To satisfy the compiler...
  def receive: Receive = { case _ => }

  def receive(kv: Map[String, String] = Map.empty[String, String],
              pendingPersists: Map[Long, (ActorRef, ActorRef)] = Map.empty[Long, (ActorRef, ActorRef)],
              expectedSeq: Long): Receive = {

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case PersistComplete(key, id) if pendingPersists.contains(id) =>
      val (client, persister) = pendingPersists.get(id).get
      context.stop(persister)
      client ! SnapshotAck(key, id)
      context.become(receive(kv, pendingPersists - id, expectedSeq))

    case PersistFailed(key, id) if pendingPersists.contains(id) =>
      val (_, persister) = pendingPersists.get(id).get
      context.stop(persister)
      context.become(receive(kv, pendingPersists - id, expectedSeq))

    case Snapshot(key, valueOption, seq) if seq == expectedSeq =>
      val newKv =
        if (valueOption.nonEmpty)
          kv + (key -> valueOption.get)
        else
          kv - key

      val persist = Persist(key, valueOption, seq)
      val persister = context.actorOf(Props(new Persister(persist, persistenceActor)))
      context.become(receive(newKv, pendingPersists + (seq -> (sender, persister)), nextExpectedSeq))

    case Snapshot(key, _, seq) if seq < expectedSeq =>
      sender ! SnapshotAck(key, seq)

    case _ =>
  }
}
