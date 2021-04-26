import java.util.concurrent.atomic.*;

import ox.cads.util.{Log,NullLog,ThreadID}

/** Adding the deleteMin operation to the binomial heap. */
abstract class Deletion(
  theLog: Log = NullLog, MaxRetries: Int = 2000,
  numMinsMin: Int = 1, numMinsDelMin: Int = 1,
  tidyRatio: Int = 8, RecTidy: Boolean = true,
  A: Int = 16, B: Int = 16,
  backtracking: Boolean = false, retrieving: Boolean = false,
  allowConcurrentUnion: Boolean = false)
    extends
    Minimum(theLog, MaxRetries, numMinsMin, numMinsDelMin, tidyRatio, 
            RecTidy, A, B, backtracking, retrieving, allowConcurrentUnion){

  /** Try to delete delNode with expected predecessor pred and state delState:
    * delNode labelling step. 
    * @return Boolean indicating whether labelling was successful. */
  protected def delete(pred: Node, delNode: Node, delState: NodeState,
                       startEpoch: Long): Boolean = {
    // Profiler.count("delete")
    // log("delete "+delNode.key+" in heap "+id)
    myAssert(delState.unlabelledParentless, delState.toString(2,2,delNode))
    val delStateL =
      if(allowConcurrentUnion)
        labelHeadForDelete(pred, delNode, delState, delState.next, startEpoch)
      else labelForDelete(pred, delNode, delState, delState.next)
    if(delStateL != null){ // labelling successful
      myAssert(delStateL.label.isInstanceOf[Delete])
      completeDelete(pred, delNode, delStateL, true); true
    }
    else false // labelling of delNode unsuccessful
  }

  /** Try to delete head with LabelForDelete label, prior to delNode labelling
    * step.
    * @return the resulting NodeState, or null if unsuccessful. */
  private def labelHeadForDelete(
    pred: Node, delNode: Node, delState: NodeState,
    newNext: Node, startEpoch: Long)
      : NodeState = {
    // log("labelForDelete"+(id, delNode.key))
    val delLabel = Delete(pred); delLabel.heap = this // label for delNode
    val label =
      LabelForDelete(delNode, delState, newNext, startEpoch, delLabel)
    // Add label to head
    if(// epoch.get == startEpoch &&
         labelHead(label, delNode, delState, startEpoch))
      completeLabelForDelete(delNode, delState, newNext, startEpoch, label)
    else null // epoch or delNode's state or heapIndirection changed
  }

  /** Complete the labelling of delNode for deletion, with concurrent unions
    * allowed.  head has been labelled with lfd.  Other threads may help with
    * this part. */
  protected def completeLabelForDelete(delNode: Node, delState: NodeState,
      newNext: Node, startEpoch: Long, lfd: LabelForDelete): NodeState = {
    var delStateL: NodeState = null
    var done = false
    // If epoch unchanged, try to label delNode
    if(epochValid(startEpoch)){
      // If a thread enters this branch, all will until lfd is removed from
      // head.  
      myAssert(delState.unlabelled)
      delStateL = new NodeState(
        parent = delState.parent, degree = delState.degree,
        children = delState.children,
        next = newNext, seq = delState.seq+1, label = lfd.delLabel)
      if(delNode.getState == delState &&
           delNode.compareAndSet(delState, delStateL))
        done = true 
    } // end of if(epochValid(startEpoch))
    // else log(ThreadID.get+": labelForDelete - epoch changed")

    // Mark label as done
    lfd.done = true; tidyHeapLabelList
    // log("delete - removed label: done = "+done)

    if(done) delStateL
    else{
      // Test if another thread did the labelling.  This needs to be outside
      // the above if, in case the main thread is delayed, and the labelling
      // of delNode and unlabelling of head are done by other threads.
      delStateL = delNode.getState
      if(delStateL.label eq lfd.delLabel){
        // log("completeLabelForDelete: another thread did labelling")
        delStateL
      }
      else null
    }

    // if(done) delStateL else null
  }

  /** Try to delete delNode with expected predecessor pred and state delState:
    * delNode labelling step, with newNext set as the new next reference.
    *  This version is used when there are no concurrent unions. 
    * @return the resulting NodeState, or null if unsuccessful. */
  private def labelForDelete(
    pred: Node, delNode: Node, delState: NodeState, newNext: Node)
      : NodeState = {
    myAssert(delState.unlabelled)
    val delLabel = Delete(pred) // Label for delNode
    val delStateL = new NodeState(
      parent = delState.parent, degree = delState.degree,
      children = delState.children,
      next = newNext, seq = delState.seq+1, label = delLabel)
    if(delNode.getState == delState){
      if(delNode.compareAndSet(delState, delStateL)) delStateL
      else{ backoff(); null } // labelling failed; retry
    }
    else{ /* backoff();*/ null } // labelling failed; retry
  }

  /** Try to delete delNode with non-null parent: delNode labelling step.
    * @param pred the expected predecessor of delNode
    * @param delNode the node to be deleted
    * @param delState the expected state of delNode
    * @param pPred the expected predecessor of parent
    * @param parent the parent of delNode within delState
    * @param pState the expected state of parent
    * @param startEpoch the epoch when this deleteMin started.
    * @return Boolean indicating whether labelling was successful. */
  protected def deleteWithParent(
    pred: Node, delNode: Node, delState: NodeState,
    pPred: Node, parent: Node, pState: NodeState, startEpoch: Long)
      : Boolean = {
    // Profiler.count("deleteWithParent")
    // log("deleteWithParent "+delNode.key)
    myAssert(delState.parent == parent && pState.label.isInstanceOf[Delete] &&
               delState.unlabelled)
    val newNext = if(delState.next == null) pState.next else delState.next
    val delStateL =
      if(allowConcurrentUnion)
        labelHeadForDelete(pred, delNode, delState, newNext, startEpoch)
      else labelForDelete(pred, delNode, delState, newNext)
    if(delStateL != null){
      myAssert(delStateL.label.isInstanceOf[Delete])
      completeDeleteWithParent(pred, delNode, pPred, parent, pState, true)
      true
    }
    else false
  }

  /** Complete the deletion of delNode with non-null parent: help with the
    * deletion of the parent.  Parameters as for deleteWithParent. */
  private def completeDeleteWithParent(
    pred: Node, delNode: Node, pPred: Node, parent: Node,
    pState: NodeState, strict: Boolean)
      : Unit = {
    // labelling succeeded; help parent first
    helpDelete(pPred, parent, pState); myAssert(parent.deleted)
    // Clear parent: this will succeed unless another thread does it
    val newDelState = delNode.maybeClearParent // FIXME?  Can this be improved?
    myAssert(newDelState.parent == null)
    // Now complete deletion
    completeDelete(pred, delNode, newDelState, strict)
  }

  /** Try to delete delNode with expected predecessor pred and state delStateL:
    * last child update step, pred update and update of children.
    * @param strict true if this node is not helping: don't update childrens'
    * parent pointers if false. */
  @inline private def completeDelete(
    pred: Node, delNode: Node, delStateL: NodeState, strict: Boolean) = {
    // log("completeDelete "+delNode.key)
    myAssert(delStateL.parent == null)
    // Profiler.count("completeDelete")
    val children = delStateL.children; val next = delStateL.next
    if(children.isEmpty) predUpdate(pred, delNode, next, children)
    else{
      // update last child
      val lastC = children.last; val lastCState = lastC.getState
      if(lastCState.parent == delNode && lastCState.next != next){
        val newCState = new NodeState(
          parent = delNode, degree = lastCState.degree,
          children = lastCState.children, next = next, seq = lastCState.seq)
        if(!(lastC.getState == lastCState &&
               lastC.compareAndSet(lastCState, newCState))){
          val cState = lastC.getState
          // Somebody else must have done the CAS: either next is set
          // appropriately, or the delete has finished.  
          myAssert(cState.next == next ||
                     cState.parent != delNode && delNode.deleted,
                   "cState = "+cState.toString(2,2,lastC)+
                     "\ndelNode = "+delNode.toString(2)+
                     "\npred = "+pred.toString(2))
        }
      } // end of if(lastCState.parent == delNode && lastCState.next != next)

      // update pred
      predUpdate(pred, delNode, children.head, children)

      // Update children, changing parent from delNode to null
      myAssert(delNode.deleted)
      if(strict) for(c <- children){
        val cState = c.getState
        if(cState.parent == delNode &&
             !c.compareAndSet(cState, cState.clearParent)){
          // Either another thread did this update, or c has been labelled for
          // deletion.
          val newCState = c.getState
          myAssert(newCState.parent != delNode ||
                     newCState.label.isInstanceOf[Delete],
                   c.toString)
        }
      }
    } // end of else
  }

  /** Update predecessor of delNode, expected to be pred, to point to next (or
    * return if another thread has done it).
    * @param children the children of delNode. */
  private def predUpdate(
    pred: Node, delNode: Node, next: Node, children: List[Node]) = {
    // Profiler.count("predUpdate")
    val predState = pred.maybeClearParent()
    // If pred still points to delNode, try to update it
    if(predState.next == delNode && predState.parent == null && 
         completePredUpdate(pred, predState, delNode, next)){ } // done
    else{
      // Profiler.count("predUpdate-search")
      // FIXME: consider spinning here
      var done = delNode.deleted; var iters = 0
      var firstSearch = true // false if we did an unsuccessful search
      // Profiler.count("newPredUpdate start search")
      while(!done){
        iters += 1;
        myAssert(iters < 100,
                 "firstSearch = "+firstSearch+"\ndelNode = "+delNode+
                   "\nnext = "+next)
        // Find new predecessor.  If the search reaches sentinel != null, then
        // it will indicate that delNode has been detached.  If
        // children.isEmpty and firstSearch, it is not possible to use such a
        // technique: the traversal might reach a node that is merged to
        // become a child of next, and then reach next. 
        val sentinel = if(children.nonEmpty || !firstSearch) next else null
        val h = if(allowConcurrentUnion) delNode.getHeap else this
        // log("findPred for "+delNode.toString(1)+
        //      " on "+h.id+" from "+id+"; firstSearch = "+firstSearch)
        // Strictly speaking, the use of h here isn't necessary, because
        // findPred itself calls delNode.getHeap.
        val (p, pState) = 
          if(firstSearch) h.unreliableFindPred(delNode, sentinel)
          else h.findPred(delNode, sentinel)
        // log("findPred done "+delNode.toString(1)+
        //       " p = "+(if(p==null) "null" else p.toString(1)))
        if(p == delNode){ assert(delNode.deleted); done = true }
        else if(p != null){
          assert(pState.next == delNode && pState.parent == null)
          done =
            delNode.deleted || completePredUpdate(p, pState, delNode, next) ||
              delNode.deleted
          // if(!done && iters > 80){
          //   if(pState.label != null) theLog.println(pState.label.toString) 
          //   else{ theLog.println("P"); assert(p.getState != pState) }
          // }
        } // end of if(p != null)
        else{  // the traverse failed to find delNode
          assert(firstSearch); firstSearch = false; done = delNode.deleted
        }
      } // end of while
    } // end of outer if
  }

  /** Try to update pred to point to next.  
    * Note: this is independent of the heap on which it is called. 
    * @param predState the expected state of pred.
    * @param delNode the node being deleted, equal to predState.next.
    * @param next the node that pred.next should be set to: either delNode's 
    * first child or delNode.next.
    * @return true if successful. */
  protected def completePredUpdate(
    pred: Node, predState: NodeState, delNode: Node, next: Node) : Boolean = {
    myAssert(predState.next == delNode && predState.parent == null,
             "pred = "+pred.toString(2)+"\ndelNode = "+delNode.toString(2))

    predState.label match{
      case null => completePredUpdate0(pred, predState, delNode, next, null)

      case hll: HeapLabelList =>
        // log("completePredUpdate from pred with label "+hll)
        val ur = hll.getUR
        if(ur != null){
          // Need to call joined on the corresponding UnionGiver
          ur.giver.head.getLabel match{
            case ug @ UnionGiver(_, ur1) if ur eq ur1 => ug.joined
            case _ => {} // UnionGiver has been removed, so union set as done
          }
        }
        completePredUpdate0(pred, predState, delNode, next, hll)

    // val pLabel = predState.label
    // pLabel match{
    //   case null | _: HeapLabelList => 
    //     if(pLabel != null){
    //       log("completePredUpdate from pred with label "+pLabel)
    //       val ur = pLabel.asInstanceOf[HeapLabelList].getUR
    //       if(ur != null){
    //         // Need to call joined on the corresponding UnionGiver
    //         ur.giver.head.getLabel match{
    //           case ug @ UnionGiver(_, ur1) if ur eq ur1 => ug.joined
    //           case _ => {} // UnionGiver has been removed, so union set as done
    //         }
    //       }
    //     }
    //     val newPredState = new NodeState(
    //       degree = predState.degree, children = predState.children,
    //       next = next, label = pLabel, seq = predState.seq)
    //     if(pred.getState == predState &&
    //          pred.compareAndSet(predState, newPredState)){ // success
    //       delNode.deleted = true; true
    //     }
    //     else if(delNode.deleted) true
    //     else{
    //       // pred's state changed
    //       val predState1 = pred.maybeClearParent()
    //       predState1.next == delNode && predState1.parent == null &&
    //         completePredUpdate(pred, predState1, delNode, next) // retry
    //     }

      case ug @ UnionGiver(receiver, ur) => // this is a small enhancement
        if(ug.joined){
          val (last, lastState, true) = ug.getLastInfo
          // A union has happened, and delNode transferred to receiver as
          // the first node of the giving heap.
          assert(ug.first == delNode && lastState.next == delNode)
          // log("completePredUpdate into "+receiver.id)
          // The following might need to help remove the UnionLast label. 
          completePredUpdate(last, lastState, delNode, next)
        }
        else{
          // log("completePredUpdate helping with UnionGiver "+ug)
          helpUG(pred, predState, ug); false
          // It would be enough to help up to the unionJoin, then to act as
          // above; but this seems more trouble than it's worth.
        }

      case l => help(pred, predState); false
    }
  }

  /** Really do the pred update. */
  private def completePredUpdate0(
    pred: Node, predState: NodeState, delNode: Node, next: Node, pLabel: Label)
      : Boolean = {
    val newPredState = new NodeState(
      degree = predState.degree, children = predState.children,
      next = next, label = pLabel, seq = predState.seq)
    if(pred.getState == predState &&
         pred.compareAndSet(predState, newPredState)){ // success
      delNode.deleted = true; true
    }
    else if(delNode.deleted) true
    else{
      // pred's state changed
      val predState1 = pred.maybeClearParent()
      predState1.next == delNode && predState1.parent == null &&
        completePredUpdate(pred, predState1, delNode, next) // retry
    }
  }

  /** Maybe uncouple the node after node if it is being deleted. */
  @inline protected def maybeUncouple(node: Node, nodeState: NodeState) = {
    if(nodeState.label == null && nodeState.parent == null){
      val next = nodeState.next
      if(next != null){
        val nextState = next.getState
        if(nextState.label.isInstanceOf[Delete] && !next.deleted &&
             nextState.parent == null){
          val children = nextState.children
          if(children.isEmpty || children.last.getState.next != null){
            val newNext = if(children.nonEmpty) children.head else nextState.next
            completePredUpdate(node, nodeState, next, newNext)
          }
        }
      }
    }
  }

  /** Try to find predecessor of delNode.
    * @param sentinel if non-null, the first child of delNode.
    * @return either: (1) the predecessor of delNode and its state; or 
    * (2) (null, null) if the predecessor was not found; or (3) (delNode,null)
    * if delNode is found to be already deleted.
    */
  protected def unreliableFindPred(delNode: Node, sentinel: Node)
      : (Node, NodeState) = /* Profiler.time("unreliableFindPred") */ {
    // Profiler.count("unreliableFindPred")
    var prev = head; var pState = prev.getState
    var curr = pState.getNextRoot(prev)
    //var retries = 0
    if(delNode.deleted) (delNode, null)
    else{
      // Traverse root list.  Inv: Either: (1) pState.next = curr; or (2)
      // pState.label is a MergeNext or MergeParent, the t2 update has been
      // applied to pState.next, and curr is the next root; or (3)
      // pState.parent = curr.  Clause (3) applies whenever pState.parent !=
      // null.
      while(curr != null && curr != delNode && curr != sentinel){
        prev = curr; pState = prev.maybeClearParent()
        if(pState.parent != null){
          if(pState.parent.getLabel.isInstanceOf[Delete]){
            curr = pState.parent
            if(curr == delNode){
              // We've reached a child other than the first.  The deletion
              // must have been completed (and the children rearranged).
              myAssert(delNode.deleted,
                       "prev = "+prev.key+"; "+
                         pState.toStringChild(2, prev, delNode)+
                         "\ndelNode = "+delNode.toString)
              return (delNode, null)
            }
          }
          else{
            // restart
            prev = head; pState = prev.getState; curr = pState.getNextRoot(prev)
            //retries += 1; if(retries >= MaxRetries-5) theLog.println(this)
            //myAssert(retries < MaxRetries, "retries = "+retries)
          }
        }
        else{
          maybeUncouple(curr, curr.getState); curr = pState.getNextRoot(prev)
        }
      } // end of while loop

      if(curr == delNode){ // p is new predecessor in root list
        if(pState.next == delNode) (prev, pState)
        else{
          myAssert(pState.parent == null)
          // Profiler.count("unreliableFindPred-help")
          // Help complete merge
          pState.label match{
            case MergeNext(_, _, state2) =>
              myAssert(state2.next == delNode); help(prev, pState)
            case MergeParent(t1, _, _, _, state2) if t1 == prev =>
              myAssert(state2.next == delNode); help(prev, pState)
            case _ => myAssert(false, "prev = "+prev.toString+"; pState = "+
                                 pState.toString(2, 2, prev))
          }
          // See if can now complete the update
          val newPState = prev.getState
          if(newPState.next == delNode && newPState.parent == null)
            (prev, newPState) // found predecessor
          else (null, null) // failed
        }
      } // end of if (curr == delNode)
      else if(curr != null){
        // The traverse reached a child of n, so the deletion must have been
        // completed by another thread.
        myAssert(curr == sentinel); delNode.deleted = true; (delNode, null)
      }
      else (null, null) // failed
    } // end of outer else
  }

  /** Try to find predecessor of delNode using strict search.  Pre: delNode
    * has a Delete label and a null parent. 
    * @param sentinel if non-null, the first child of delNode.
    * @return optionally the predecessor of delNode and its state; or None 
    * if delNode is not found. */
  protected def findPred0(delNode: Node, sentinel: Node)
      : Option[(Node, NodeState)] = {
    var curr = head; var currSeq = curr.getSeq // current node and seq num
    var prev: Node = null; var pState: NodeState = null // prev node, state
    var found = false // have we found the node?
    // var iters = 0; val Limit = 2*MaxRetries
    @inline def restart() = {
      if(delNode.deleted) curr = null
      else{ curr = head; currSeq = curr.getSeq /* ; restarts += 1 */ }
    }
    while(!found && curr != null && curr != sentinel){
      // iters += 1; myAssert(iters < Limit)
      advance(curr, currSeq) match{
        case null => restart()
        case (next, nextSeq, skipNodes) =>
          // Test if delNode in skipNodes
          prev = curr; var sn = skipNodes
          while(sn.nonEmpty && sn.head != delNode){
            prev = sn.head; sn = sn.tail
          }
          if(sn.nonEmpty){ // sn.head == delNode
            pState = prev.maybeClearParent()
            if(pState.next != delNode){
              if(prev == curr && pState.label != null){
                // Probably curr has MergeNext/MergeParent label, and merge
                // needs completing; could be more sophisticated here.
                help(prev, pState)
                pState = prev.maybeClearParent()
                if(pState.next == delNode && pState.parent == null) found = true
                else restart()
              }
              else restart()
            }
            else if(pState.parent == null) found = true
            else{ 
              // Need to help with deletion of pState.parent.  It must have
              // been decoupled before the call to advance that reached prev,
              // but has not yet been fully deleted.
              val parent = pState.parent; val parentState = parent.getState
              val pPrev = parentState.label.asInstanceOf[Delete].prev
              // Profiler.count("findPred-helpParent")
              helpDelete(pPrev, parent, parentState)
              pState = prev.maybeClearParent()
              if(pState.next == delNode && pState.parent == null) found = true 
              else{ assert(pState.parent != parent); restart() }
            }
          } // end of if(sn.nonEmpty)
          else{ curr = next; currSeq = nextSeq }
      }
    } // end of while loop
    if(found) Some((prev, pState)) else None
  }

  /** Try to find predecessor of delNode using strict search.  Pre: delNode
    * has a Delete label and a null parent. 
    * @param sentinel if non-null, the first child of delNode.
    * @return the predecessor of delNode and its state; or (delNode, null) 
    * if delNode is found to be already deleted. */
  protected def findPred(delNode: Node, sentinel: Node)
      : (Node, NodeState) = /* Profiler.time("findPred") */{
    // Profiler.count("findPred")
    // log("findPred"+(delNode, sentinel)+" in "+id)
    val startEpoch = epoch.get
    val h = if(allowConcurrentUnion) delNode.getHeap else this
    if(h != this){
      // log("recursive findPred for "+delNode.toString(1)+" from "+id+" to "+h.id)
      h.findPred(delNode, sentinel)
    }
    else findPred0(delNode, sentinel) match{
      case Some((prev, pState)) => (prev, pState)
      case None =>
        if(allowConcurrentUnion && epoch.get != startEpoch){
          if(delNode.deleted){
            // log("findPred: epoch changed, but already deleted")
            (delNode, null)
          }
          else{
            val h = delNode.getHeap
            // log("Epoch changed from "+startEpoch+" to "+epoch.get+
            //       ".  Recursive findPred for "+delNode.toString(1)+" from "+id+
            //       " to "+h.id)
            h.findPred(delNode, sentinel)
          }
        }
        else{  // delNode must have been detatched from the root list
          val delNodeSt = delNode.toString
          // if(delNode.deleted)
          //   log("Failed to find "+delNodeSt+" in findPred: already deleted")
          // else
          //   log("Failed to find "+delNodeSt+" in findPred: setting deleted.")
          // if(allowConcurrentUnion) log("this = "+id+"; delNode in "+delNode.getHeap)
          delNode.deleted = true; (delNode, null)
        }
    }
  }

  /** Help with a Delete label, if the previous node has appropriate state.
    * @param pred the previous node.
    * @param helpNode the node with the Delete label.
    * @param helpState a recent state of helpNode, with label delLabel. */
  @inline protected
  def helpDelete(pred: Node, helpNode: Node, helpState: NodeState) = {
    if(!helpNode.deleted){ /* Profiler.time("newCompleteDelete help")*/
      // Profiler.count("helpDelete")
      val parent = helpState.parent
      if(parent != null){
        // Need to help with deletion of parent
        val pState = parent.getState; val Delete(pPred) = pState.label
        completeDeleteWithParent(pred, helpNode, pPred, parent, pState, false)
      }
      else completeDelete(pred, helpNode, helpState, false)
    } 
  }

}
