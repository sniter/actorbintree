/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor.*
import scala.collection.immutable.Queue
import scala.util.Random

object BinaryTreeSet:

  trait Operation:
    def requester: ActorRef
    def id: Int
    def elem: Int

  trait OperationReply:
    def id: Int

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

  /** Request to perform garbage collection */
  case object GC
  case object Terminate 

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply



class BinaryTreeSet extends Actor:
  import BinaryTreeSet.*
  import BinaryTreeNode.*

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = { 
    case op: Operation=> root ! op
    case GC => 
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */  
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation=> root ! op
    case CopyFinished => 
      val oldRoot = root
      root = newRoot
      oldRoot ! Terminate
      context.become(normal, discardOld = true)
  }


object BinaryTreeNode:
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor:
  import BinaryTreeNode.*
  import BinaryTreeSet.*

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  def evaluate(value: Int): Option[Position] = 
    math.signum(value - elem) match { 
      case 0 => None
      case 1 => Some(Right)
      case -1 => Some(Left)
    }

  val handleOps: Receive = {
    case cmd @ Insert(requester, id, newEl) => evaluate(newEl) match {
      case None =>
        if (removed){
          removed = false
        }
        requester ! OperationFinished(id)
      case Some(child) =>
        subtrees.get(child).fold{
          val newChildActor = context.actorOf(props(newEl, initiallyRemoved = false))
          subtrees += (child, newChildActor)
          requester ! OperationFinished(id)
        } { subtree =>
          subtree ! cmd
        }
    }
    case containsMsg @ Contains(requester, id, el) => evaluate(el) match {
      case None =>
        requester ! ContainsResult(id, !removed)
      case Some(child) =>
        subtrees.get(child).fold {
          requester ! ContainsResult(id, false)
        } { subtree =>
          subtree ! containsMsg
        }
    }
    case cmd @ Remove(requester, id, el) => evaluate(el) match {
      case None =>
        removed = true
        requester ! OperationFinished(id)
      case Some(child) =>
        subtrees.get(child).fold {
          requester ! OperationFinished(id)
        } { child =>
          child ! cmd
        }
    }
  }

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = handleOps.orElse{
    case msg @ CopyTo(newRoot) =>
      val expected = subtrees.values.toSet + sender
      val insertConfirmed = removed
      context.become(copying(expected, insertConfirmed))
      subtrees.foreach{ 
        case (_, subtree) => subtree ! CopyTo(newRoot)
      }
      if (!insertConfirmed)
        newRoot ! Insert(self, Random.nextInt, elem)
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = handleOps.orElse{
    case OperationFinished(_) => 
      if (expected.size > 1)
        context.become(copying(expected, insertConfirmed = true))
      else
        expected.head ! CopyFinished
    case CopyFinished if (expected.size > 1) => 
        val newExpected = expected - sender
        val newInsertConfirmed = newExpected.size == 1
        context.become(copying(expected = newExpected, insertConfirmed = newInsertConfirmed))
    case CopyFinished if insertConfirmed => 
      expected.head ! CopyFinished
    case Terminate =>
      subtrees.foreach{
        case (position, actor) => actor ! Terminate
      }
      context.stop(self)
    case _ => ()
  }


