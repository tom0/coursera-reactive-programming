/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

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

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case i @ Insert(_, _, _) => root ! i
    case c @ Contains(_, _, _) => root ! c
    case _ =>
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = ???

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

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case i @ Insert(actorRef, id, newElem) if newElem > elem => {
      println("&&&&&&& Insert " + newElem) 
      if (subtrees.get(Right).nonEmpty) {
        subtrees.get(Right).get ! i
      } else {
        val newActor = context.actorOf(Props(classOf[BinaryTreeNode], newElem, false))
        subtrees = subtrees + { Right -> newActor }
        actorRef ! OperationFinished(id)
      }
    }
    case i @ Insert(actorRef, id, newElem) if newElem < elem => {
      println("&&&&&&& Insert " + newElem) 
      if (subtrees.get(Left).nonEmpty) {
        subtrees.get(Left).get ! i
      } else {
        val newActor = context.actorOf(Props(classOf[BinaryTreeNode], newElem, false))
        subtrees = subtrees + { Left -> newActor }
        actorRef ! OperationFinished(id)
      }
    }
    case Insert(actorRef, id, newElem) => actorRef ! OperationFinished(id)
    case Contains(actorRef, id, elemToCheck) if elemToCheck == elem => {
      println("***** contains " + elemToCheck)
      actorRef ! ContainsResult(id, true)
    }
    case c @ Contains(actorRef, id, elemToCheck) if elemToCheck < elem => {
      println("***** contains " + elemToCheck)
      if (subtrees.get(Left).nonEmpty) {
        subtrees.get(Left).get ! c
      } else {
        actorRef ! ContainsResult(id, false)
      }
    }
    case c @ Contains(actorRef, id, elemToCheck) if elemToCheck > elem => {
      println("***** contains " + elemToCheck)
      if (subtrees.get(Right).nonEmpty) {
        subtrees.get(Right).get ! c
      } else {
        actorRef ! ContainsResult(id, false)
      }
    }
    case r @ Remove(actorRef, id, newElem) =>
    {

    }
    case _ =>
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???

}