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
  context.become(properReceive())

  def receive: Receive = { case _ => }

  type ReplicateId = Long
  type OriginalRequestId = Long
  type OriginalClient = ActorRef
  type SecondaryReplica = ActorRef
  type SecondaryReplicator = ActorRef

  def properReceive(
    kv: Map[String, String] = Map.empty[String, String],
    pendingPersists: Set[OriginalRequestId] = Set.empty[OriginalRequestId],
    replicasAndReplicators: Map[SecondaryReplica, SecondaryReplicator] = Map.empty[SecondaryReplica, SecondaryReplicator],
    replicasToReplicateIds: Map[SecondaryReplica, Set[ReplicateId]] = Map.empty[SecondaryReplica, Set[ReplicateId]],
    replicateIdsToOriginalRequestIds: Map[ReplicateId, OriginalRequestId] = Map.empty[ReplicateId, OriginalRequestId],
    pendingReplications: Map[OriginalRequestId, (OriginalClient, Map[ReplicateId, SecondaryReplica])]
      = Map.empty[OriginalRequestId, (OriginalClient, Map[ReplicateId, SecondaryReplica])]): akka.actor.Actor.Receive = {

    case Insert(key, value, id) =>
      // Starting a new Persister will commence persistence.
      // The persister will stop itself on success OR failure.
      context.actorOf(Props(new Persister(new Persist(key, Some(value), id), persistenceActor)))
      val newKv = kv + (key -> value)
      val newPendingPersists = pendingPersists + id

      val (newReplicasToReplicateIds, newReplicateIdsToOriginalRequestIds, newPendingReplications) =
        replicate(key, Some(value), id, replicasAndReplicators, replicasToReplicateIds, replicateIdsToOriginalRequestIds, pendingReplications)

      context.become(
        properReceive(
          newKv,
          newPendingPersists,
          replicasAndReplicators,
          newReplicasToReplicateIds,
          newReplicateIdsToOriginalRequestIds,
          newPendingReplications))

    case Remove(key, id) =>
      context.actorOf(Props(new Persister(new Persist(key, None, id), persistenceActor)))
      val newKv = kv - key
      val newPendingPersists = pendingPersists + id

      val (newReplicasToReplicateIds, newReplicateIdsToOriginalRequestIds, newPendingReplications) =
        replicate(key, None, id, replicasAndReplicators, replicasToReplicateIds, replicateIdsToOriginalRequestIds, pendingReplications)

      context.become(
        properReceive(
          newKv,
          newPendingPersists,
          replicasAndReplicators,
          newReplicasToReplicateIds,
          newReplicateIdsToOriginalRequestIds,
          newPendingReplications))

    case Persister.PersistComplete(key, seq) =>

      // Check if replication has finished.
      val finishedReplication =
        pendingReplications
          .get(seq)
          .filter {
            case (originalClient, replicateIdToReplica) => replicateIdToReplica.isEmpty
          }

      // Send the ack if replication has finished.
      finishedReplication
        .foreach {
          case (originalClient, _) => originalClient ! OperationAck(seq)
        }

      // Remove the pending replication if necessary.
      val newPendingReplications = if (finishedReplication.isEmpty) pendingReplications else pendingReplications - seq

      context.become(
        properReceive(
          kv,
          pendingPersists - seq,
          replicasAndReplicators,
          replicasToReplicateIds,
          replicateIdsToOriginalRequestIds,
          newPendingReplications))

    case Persister.PersistFailed(key, seq) =>
      val outstandingReplications = pendingReplications.get(seq)

      val replicateIdsToReplicas = outstandingReplications.map {
        case (originalClient, mapOfReplicateIdsToReplicas) => mapOfReplicateIdsToReplicas
      }.getOrElse(Map.empty[ReplicateId, SecondaryReplica])

      // Get the updated values for replicasToReplicateIds (i.e. without this replicateId).
      val updatedReplicasToReplicateIds = replicateIdsToReplicas.map {
        case (rId, secondaryReplica) => (secondaryReplica, replicasToReplicateIds.getOrElse(secondaryReplica, Set.empty[ReplicateId]) - rId)
      }

      // Out of the updated values, which are empty?
      val emptyReplicasToReplicateIds = updatedReplicasToReplicateIds.filter {
        case (_, rId) => rId.isEmpty
      }.map {
        case (secondaryReplica, _) => secondaryReplica
      }

      // Use the updated and empty values to update and remove respectively.
      val newReplicasToReplicateIds = (replicasToReplicateIds ++ updatedReplicasToReplicateIds) -- emptyReplicasToReplicateIds
      val newReplicateIdsToOriginalRequestIds = replicateIdsToOriginalRequestIds -- replicateIdsToReplicas.keySet
      val newPendingReplications = pendingReplications - seq

      context.become(
        properReceive(
          kv,
          pendingPersists - seq,
          replicasAndReplicators,
          newReplicasToReplicateIds,
          newReplicateIdsToOriginalRequestIds,
          newPendingReplications))

      outstandingReplications.foreach {
        case (originalClient, _) => originalClient ! OperationFailed(seq)
      }

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

//    case Replicas(newReplicas) =>
//      val newReplicasWithoutSelf = newReplicas.filter( _ != self )
//      val currentReplicas = replicasAndReplicators.keySet
//      val addedReplicas = newReplicasWithoutSelf.diff(currentReplicas)
//      val removedReplicas = currentReplicas.diff(newReplicasWithoutSelf)
//
//      val addedReplicasAndReplicators = addedReplicas.map(r => r -> context.actorOf(Props(new Replicator(r))))
//
//      val replicationsToSend =
//        for {
//          addedReplicaAndReplicator <- addedReplicasAndReplicators
//          kvPair <- kv
//        } yield Replicate(kvPair._1, Some(kvPair._2), nextReplicateId)
//
//      // Set the new state.
//      val newReplicasAndReplicators = (replicasAndReplicators -- removedReplicas) ++ addedReplicasAndReplicators
//      val newPendingReplications = pendingReplications ++ replicationsToSend
//      context.become(receive(kv, newReplicasAndReplicators, newPendingReplications))
//
//    case Replicated(key, id) =>
//      val originalRequestId =
//        replicateIdsToOriginalRequestIds
//          .get(id)
//          .foreach {
//            case (originalClient, setOfReplicateIds) =>
//              // TODO: Check if the change has been persisted.
//          }

    case _ =>


  }


  def replicate(key: String,
                value: Option[String],
                originalRequestId: Long,
                replicasAndReplicators: Map[SecondaryReplica, ActorRef],
                replicasToReplicateIds: Map[SecondaryReplica, Set[ReplicateId]],
                replicateIdsToOriginalRequestIds: Map[ReplicateId, OriginalRequestId],
                pendingReplications: Map[OriginalRequestId, (OriginalClient, Map[ReplicateId, SecondaryReplica])]):
    (Map[SecondaryReplica, Set[ReplicateId]],
     Map[ReplicateId, OriginalRequestId],
     Map[OriginalRequestId, (OriginalClient, Map[ReplicateId, SecondaryReplica])]) = {
    // Create the replications to be done.
    val replications = replicasAndReplicators.map {
      case (replica, replicator) => (replica, replicator, new Replicate(key, value, nextReplicateId))
    }

    // Update all of the state.
    val repToRepId: Iterable[(SecondaryReplica, Set[ReplicateId])] = replications.map {
      case (replica, _, replicate) => replica -> (replicasToReplicateIds.getOrElse(replica, Set.empty[ReplicateId]) ++ Set(replicate.id))
    }
    val newReplicasToReplicateIds = replicasToReplicateIds ++ repToRepId

    val newReplicateIdsToOriginalRequestIds = replicateIdsToOriginalRequestIds ++ replications.map {
      case (_, _, replicate) => replicate.id -> originalRequestId
    }

    val newPendingReplications = pendingReplications +
      (originalRequestId ->
        (sender, replications.map { case (replica, _, replicate) => replicate.id -> replica }.toMap))

          // Send the replications
    replications.foreach {
      case (_, replicator, replicate) => replicator ! replicate
    }

    (newReplicasToReplicateIds, newReplicateIdsToOriginalRequestIds, newPendingReplications)
  }
}