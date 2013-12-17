/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue
import scala.annotation.tailrec

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  // optional
  def receive = normal(createRoot)

  // optional
  /** Accepts `Operation` and `GC` messages. */
  def normal(root: ActorRef): Receive = {
    case GC => {
      println("BinaryTreeSet: normal GC")
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot, root, Queue.empty[Operation]))
    }
    case operation: Operation => {
      println("BinaryTreeSet: normal Operation: " + operation)
      root ! operation
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef, oldRoot: ActorRef, pending: Queue[Operation]): Receive = {
    case operation: Operation => {
      println("BinaryTreeSet: garbageCollecting Operation: " + operation)
      context.become(garbageCollecting(newRoot, oldRoot, pending :+ operation))
    }
    case CopyFinished => {
      println("BinaryTreeSet: garbageCollecting CopyFinished")

      @tailrec
      def drainQueue(queue: Queue[Operation]): Unit = {
        if (queue.nonEmpty) {
          newRoot ! queue.head
          drainQueue(queue.tail)
        }
      }

      oldRoot ! PoisonPill
      context.become(normal(newRoot))
      drainQueue(pending)
    }
    case GC => // Ignore, as per the spec
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  // optional
  def receive = normal(Map[Position, ActorRef](), initiallyRemoved)

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  def normal(subtrees: Map[Position, ActorRef], removed: Boolean): Receive = {

    case msg => {
      def getSubtreeFor(elemToCheck: Int): Option[ActorRef] = {
        getPositionFor(elemToCheck).flatMap { pos =>
          subtrees.get(pos)
        }
      }

      def getPositionFor(elemToCheck: Int): Option[Position] = {
        if (elemToCheck < elem) {
          Some(Left)
        } else if (elemToCheck > elem) {
          Some(Right)
        } else None
      }

      msg match {
        case CopyTo(actorRef) => {
          if (subtrees.isEmpty && removed) {
            context.parent ! CopyFinished
          }

          context.become(copying(subtrees.values.toSet, removed))
          subtrees.foreach { case (_, tree) =>
            tree ! CopyTo(actorRef)
          }
          if (!removed) {
            actorRef ! Insert(self, elem, elem)
          }
        }
        case i @ Insert(actorRef, id, newElem) if newElem != elem => {
          val subTree = getSubtreeFor(newElem)
          if (subTree.nonEmpty) {
            subTree.get ! i
          } else {
            val newActor = context.actorOf(Props(classOf[BinaryTreeNode], newElem, false))
            context.become(normal(subtrees + { getPositionFor(newElem).get -> newActor }, removed))
            actorRef ! OperationFinished(id)
          }
        }
        case Insert(actorRef, id, _) => {
          context.become(normal(subtrees, removed = false))
          actorRef ! OperationFinished(id)
        }
        case c @ Contains(actorRef, id, elemToCheck) if elemToCheck != elem => {
          val subTree = getSubtreeFor(elemToCheck)
          if (subTree.nonEmpty) {
            subTree.get ! c
          } else {
            actorRef ! ContainsResult(id, result = false)
          }
        }
        case Contains(actorRef, id, _) => {
          actorRef ! ContainsResult(id, !removed)
        }
        case r @ Remove(actorRef, id, elemToRemove) if elemToRemove != elem => {
          val subTree = getSubtreeFor(elemToRemove)
          if (subTree.nonEmpty) {
            subTree.get ! r
          } else {
            actorRef ! OperationFinished(id)
          }
        }
        case r @ Remove(actorRef, id, _) => {
          context.become(normal(subtrees, removed = true))
          actorRef ! OperationFinished(id)
        }
      }
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case CopyFinished => becomeCopyingOrFinish(expected - sender, insertConfirmed)
    case OperationFinished(_) => becomeCopyingOrFinish(expected, insertConfirmed = true)
  }

  def becomeCopyingOrFinish(expected: Set[ActorRef], insertConfirmed: Boolean) {
    if (expected.isEmpty && insertConfirmed) {
      context.parent ! CopyFinished
    } else {
      context.become(copying(expected, insertConfirmed))
    }
  }
}
