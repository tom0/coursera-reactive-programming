package kvstore

import akka.actor.{ActorRef, Actor, Props}
import kvstore.Replica._
import kvstore.Replica.OperationFailed
import kvstore.Replica.Remove
import kvstore.Replicator.{Replicated, Replicate}
import kvstore.Persistence.Persist
import scala.Some
import kvstore.Replica.OperationAck
import kvstore.Arbiter.Replicas
import kvstore.Replica.Insert

class PrimaryReplica(persistenceProps: Props) extends Actor {

  var replicateId = 0L
  def nextReplicateId = {
    val ret = replicateId
    replicateId += 1
    ret
  }

  val persistenceActor = context.actorOf(persistenceProps)
  context.become(receive())

  def receive: Receive = { case _ => }

  def receive(kv: Map[String, String] = Map.empty[String, String],
              replicasAndReplicators: Map[ActorRef, ActorRef] = Map.empty[ActorRef, ActorRef], // Replica -> Replicator
              pendingReplicationsClientLookup: Map[Long, Long] = Map.empty[Long, Long],
              pendingReplications: Map[Long, (ActorRef, Set[Long])] = Map.empty[Long, (ActorRef, Set[Long])]): Receive = {

    case Insert(key, value, id) =>
      val persist = Persist(key, Some(value), id)
      val persister = context.actorOf(Props(new Persister(persist, persistenceActor)))
      context.become(receivePersist(kv, sender, persister, persist))

    case Remove(key, id) =>
      val persist = Persist(key, None, id)
      val persister = context.actorOf(Props(new Persister(persist, persistenceActor)))
      context.become(receivePersist(kv, sender, persister, persist))

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

//    case Replicas(newReplicas) =>
//      val newReplicasWithoutSelf = newReplicas.filter( _ != self )
//      val currentReplicas = replicasAndReplicators.keySet
//      val addedReplicas = newReplicasWithoutSelf.diff(currentReplicas)
//      val removedReplicas = currentReplicas.diff(newReplicasWithoutSelf)
//
//      val addedReplicasAndReplicators = addedReplicas.map(r => r -> context.actorOf(new Replicator(r)))
//
//      val replicationsToSend = kv map {
//        case (key, value) => Replicate(key, value, nextReplicateId)
//      }
//
//      // All KV pairs need to go to all the new replicas (hope there aren't lots of each!)
//      for {
//        addedReplicaAndReplicator <- addedReplicasAndReplicators
//        replication <- replicationsToSend
//      } addedReplicaAndReplicator._2 ! replication
//
//      // TODO: Handle the removed replicas.
//      // TODO: Any outstanding replications, just send a message to ourselves confirming it.
//
//
//      // Set the new state.
//      val newReplicasAndReplicators = (replicasAndReplicators -- removedReplicas) ++ addedReplicasAndReplicators
//      val newPendingReplications = pendingReplications union replicationsToSend
//
//      context.become(receive(kv, newReplicasAndReplicators, newPendingReplications))
//
//    case Replicated(key, id) =>
//      pendingReplications.get(id).foreach(pr => pr ! OperationAck)
//          .filter(_._1 == id)
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

