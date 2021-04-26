import java.util.concurrent.atomic.*;

import ox.cads.util.{Log,NullLog,ThreadID}

/** A concurrent binary heap. 
  * @param theLog a Log to use for loging.
  * @param MaxRetries the maximum number of retries that loops should attempt.
  * @param numMinsMin the number of minimum values to record while minimum is 
  * traversing.
  * @param numMinsDelMin the number of minimum values to record while deleteMin
  * is traversing.
  * @param tidyRatio the recipricol of the probability with which a thread 
  * should attempt to tidy the heap when it completes an operation. 
  * @param backtracking does deleteMin backtrack if it finds the current node's
  * sequence number has changed? 
  * @param allowUnion is a union operation allowed concurrently with other 
  * operations on the giving heap. */
class ConcHeap(
  theLog: Log = NullLog, MaxRetries: Int = 2000,
  numMinsMin: Int = 1, numMinsDelMin: Int = 1,
  tidyRatio: Int = 8, RecTidy: Boolean = true,
  A: Int = 16, B: Int = 16,
  backtracking: Boolean = false, retrieving: Boolean = false,
  allowUnion: Boolean = false)
    extends
    Union(theLog, MaxRetries, numMinsMin, numMinsDelMin, tidyRatio,
          RecTidy, A, B, backtracking, retrieving, allowUnion){

  /** An object referencing this heap. */
  protected val heapIndirection =
    new java.util.concurrent.atomic.AtomicReference(new HeapIndirection(this))
 
  // ======== heapOf

  /** What heap are the nodes reachable from head in?  If this heap is currently
    * the giving heap in a union, and that union has been linearized, then the
    * noes are part of the corresponding receiving heap. */
  def heapOf: ConcHeap = head.getLabel match{
    case ug @ UnionGiver(receiver,_) =>
      if(ug.joined) receiver.asInstanceOf[ConcHeap] else this
    case _ => this
  }

  // ========= Helping

  /** Help the label on helpNode, whose state is helpState.  Pre: the label is
    * not null.  Note: this should be independent of the heap on which it is
    * called.  The majority of the cases call into functions that do not
    * access heap variables, such as head; exceptions are UnionGiver and
    * UnionReceiver; in addition, predUpdate (called indirectly via
    * helpDelete) is careful to search in the correct heap. */
  @noinline protected[concurrent] def help(helpNode: Node, helpState: NodeState) = {
    //Profiler.count("help")
    helpState.label match{
      case Delete(pred) => helpDelete(pred, helpNode, helpState)
   
      case MergeParent(pred, pState, pLabel, t2, state2) =>  // t1 = helpNode
        if(t2.getState.parent == helpNode){  // t2 update has happened
          val newPState = pred.getState 
          if(helpNode == pred)
            mergeUpdateT1Pred(helpNode, helpState, t2, state2.next)
          else if(newPState.label eq pLabel) // pred update has not happened
            mergeUpdatePred(
              helpNode, helpState, pred, newPState, t2, state2.next)
          else // pred update has happened
            mergeUpdateT1(helpNode, helpState, pred, t2)
        }
        else{
          val newPState = pred.getState
          if(helpNode != pred && (newPState.label ne pLabel))
            // pred labelling step needs to be done, or operation has completed.
            mergeLabelPred(helpNode, helpState, pred, pState, pLabel, t2, state2)
          else
            // t1 == pred || newPState.label eq pLabel.  pred labelling step
            // is skipped or has been done.  t2 update hasn't happened yet (so
            // should help), or interference on t2 has happened (so should
            // backtrack), or maybe (when t1 == pred) the operation has
            // completed (nothing to do).
            mergeUpdateT2(helpNode, helpState, pred, newPState, t2, state2)
        }

      case pLabel @ MergeNext(t1, t2, state2) => 
        myAssert(t1 != helpNode); val newState1 = t1.getState
        newState1.label match{
          case MergeParent(`helpNode`, _, `pLabel`, `t2`, `state2`) => 
            // t1 update hasn't happened yet
            if(t2.getState.parent == t1)  // t2 update has happened
              mergeUpdatePred(
                t1, newState1, helpNode, helpState, t2, state2.next)
            else
              // t2 update hasn't happened, or interference on t2 had
              // happened, or maybe the operation has completed.
              mergeUpdateT2(t1, newState1, helpNode, helpState, t2, state2)
          case _ => { }  // t1 update has happened
        }
        
      case Inserted(node, ia) =>
        // log("Helping with Inserted "+node.key)
        assert(helpState.next == node)
        clearInserted(node, helpNode, helpState, ia)

      case UnionLast(ug)  => 
        // log("Helping with UnionLast"+helpNode.toString(2))
        ug.setDone(helpNode, helpState)
        clearLastLabel(helpNode, helpState)

      case ug: UnionGiver => helpUG(helpNode, helpState, ug)

      case hll: HeapLabelList =>
        // log("Helping with "+hll)
        hll.close
    } // end of helpState.label match
  }


  /** Is this heap labelled with ur? */
  private def labelledWith(ur: UnionReceiver): Boolean =
    head.getState.labelledWith(ur)

  /** Help with a UnionGiver label. 
    * @param helpNode the node with the Delete label.
    * @param helpState a recent state of helpNode, with label delLabel.
    * @param ug the UnionGiver label. */
  protected def helpUG(helpNode: Node, helpState: NodeState, ug: UnionGiver)
    = {
    val UnionGiver(r, rstatus) = ug
    val giver = rstatus.giver.asInstanceOf[ConcHeap]
    val receiver = r.asInstanceOf[ConcHeap]
    // if(giver != this) log("giver = "+giver.id+"; this = "+this.id)
    // I don't understand why the "asInstanceOf" is necessary.
    if(ug.joined){
      // log("helping with clearLast: "+ug)
      receiver.clearLast(giver, ug)
    }
    else if(receiver.labelledWith(rstatus)){ // receiver labelling done
      // log("helping with unionJoin "+ug)
      receiver.unionJoin(giver, ug.first, ug)
    }
    else if(ug.aborted){
      // log(" helping to replace label")
      // Note: the UnionGiver corresponding to ug.replacedBy was initialised
      // before ug was aborted.
      giver.replaceLabel(ug, ug.replacedBy)
    }
    else{
      if(!ug.isInit){
        // log("helping to initialise "+ug)
        giver.initUG(ug, helpState.next)
      }
      // log("helping with unionLabelreceiver on "+receiver.id+": "+ug)
      receiver.unionLabelReceiver(giver, ug.first, ug, true)
    }
  }

  /** Help with a HeapLabel. */
  protected[concurrent] def helpHL(label: HeapLabel) = label match{
    case ib @ InsertBelow(node, parent, parentState, epoch) =>
      // log("Helping with insertBelow of "+node.key+" in "+id)
      completeInsertBelow(node, parent, parentState, ib)

    case ia @ InsertAtEnd(node, pred, predState, epoch) =>
      // log("Helping with insertAfter of "+node.key+" in "+id+
      //       "\n pred = "+pred.toString(2))
      val res = completeInsertAtEnd(node, pred, predState, ia)
      assert(ia.done)

    case lfd @ LabelForDelete(delNode, delState, newNext, startEpoch, delLabel) =>
      // log("Helping with LabelForDelete "+lfd)
      completeLabelForDelete(delNode, delState, newNext, startEpoch, lfd)
  }

  /** Help to complete a UnionReceiver heap label. */
  protected[concurrent] def helpUnionReceiver(ur: UnionReceiver) = {
    val giver = ur.giver.asInstanceOf[ConcHeap]
    giver.head.getLabel match{
      case ug @ UnionGiver(receiver, ur1) if ur1 eq ur =>
        if(ug.joined){
          // log("helping with clearLast from "+ur)
          receiver.asInstanceOf[ConcHeap].clearLast(giver, ug)
          assert(ur.done)
        }
        else{
          // log("helping with unionJoin from "+ur)
          receiver.asInstanceOf[ConcHeap].unionJoin(giver, ug.first, ug)
          assert(ur.done)
        }
      case _ => // That union is all over except for clearing ur
        // log("helping with "+ur+"; UnionGiver removed")
        ur.done = true
    }
  }

}

package binomialHeap.concurrent

import ox.cads.util.{Log,NullLog,ThreadID}

/** Adding the minimum operation to the binomial heap. */
abstract class Insert(
  theLog: Log = NullLog, MaxRetries: Int = 2000,
  numMinsMin: Int = 1, numMinsDelMin: Int = 1,
  tidyRatio: Int = 8, RecTidy: Boolean = true,
  A: Int = 16, B: Int = 16,
  backtracking: Boolean = false, retrieving: Boolean = false,
  allowConcurrentUnion: Boolean = false)
    extends
    Tidy(theLog, MaxRetries, numMinsMin, numMinsDelMin, tidyRatio, 
         RecTidy, A, B, backtracking, retrieving, allowConcurrentUnion){

  // ========= Insertion

  /** Given the t1, t2 and state2 components of a MergeNext label on a node curr
    * (or MergeParent label with t1=curr), return a subsequent root, possibly
    * skipping over t2, or null if curr is the last root.  The returned node
    * was a root node until the label was removed from curr. */
  private def skipOverMerge(t1: Node, t2: Node, state2: NodeState) : Node = {
    val myState2 = t2.getState; val next = state2.next
    if(next != null){ // t2 not last node
      if(myState2 == state2 || myState2.parent == t1) next
        // Skip over t2; next is the next root in either case.
      else t2 // (next might have been moved)
    }
    else if(myState2.parent == t1)//t2 update has happened; curr is last root
      null
    else t2 // t2 is next root
  }

  /** Insert key into the heap. */
  def insert(key: Int) = /* Profiler.time("insert") */ {
    // Create node
    val myNode = new Node(key)
    var curr = head; var currState : NodeState = null // current node, state
    var done = false
    var maxDelay = 3200 // max delay in binary backoff
    val Limit = 200000
    // # restarts, and limit on how many times restarts allowed
    var restarts = 0; val RestartLimit = 4
    var iters = 0
    // var restartCount = 0;
    // var iterCount = 0
    // Restart traversal
    def restart() = {
      restarts += 1; curr = head
      // The following gives a false failure with probability ~(3/4)^200.
      // myAssert(restarts < 200)
    }

    // Try to skip over the next node if it is being merged.
    @inline def skip(t1: Node, t2: Node, state2: NodeState) = {
      // Profiler.count("insert-skip")
      val next = skipOverMerge(t1, t2, state2)
      if(next == null){
        if(restarts < RestartLimit || random.nextInt(4) != 0) restart()
        else{
          // Profiler.count("helpMerge")
          help(curr, currState); restarts = 0; curr = head
        } // curr is last root
      }
      else curr = next
    }

    while(!done){
      // iterCount += 1
      // if(iterCount%10000 == 0)
      //   theLog.println(ThreadID.get+" insert:"+iterCount+"\ncurr = "+curr.toString+
      //             "\nhead = "+head.toString(50))
      // myAssert(iterCount <= 100000)
      // Profiler.count("iter"+ThreadID.get)
      currState = curr.maybeClearParent(); val parent = currState.parent
      if(allowConcurrentUnion && curr != head && curr.getHeap != this){
        // log(ThreadID.get+": insert("+id+", "+key+") restarting\n"+
        //       "  curr = "+curr.toString(3)+"; curr heap = "+curr.getHeap.id+
        //       "\n  "+this)
        val headState = head.getState
        headState.label match{
          case ug : UnionGiver =>
            // log("insert helping with union: "+ug)
            helpUG(head, headState, ug)
          case l => {}
        }
        restart
      }
      else if(parent != null) curr = parent
      else if(currState.label == null){
        if(currState.next == null){ // try to append after last node
          if(insertAtEnd(myNode, curr, currState)) done = true
          else{
            // Profiler.count("insert end CAS fail")
            insertSpin(maxDelay)
            if(maxDelay >= Limit && random.nextInt(2) != 0) curr = head
            else maxDelay = (2*maxDelay) min Limit
          }
        } // end of if(currState.next == null)
        else if(currState.degree == 0 && curr.key <= key && curr != head){
          // try to insert myNode below curr
          myNode.setParentInitial(curr)
          if(insertBelow(myNode, curr, currState)) done = true
          else myNode.clear
        }
          // done = insertBelow(myNode, curr, currState)
        else{ maybeUncouple(curr, currState); curr = currState.next }
      } // end of if(currState.label == null)
      else currState.label match{
        case MergeNext(t1, t2, state2) => skip(t1, t2, state2)
        case MergeParent(pred, _, _, t2, state2) => 
          if(curr == pred) skip(curr, t2, state2)
          else if(currState.next != null) curr = currState.next
          else if(restarts < RestartLimit || random.nextInt(8) != 0) restart() // at last node, with label
          else{ 
            // Profiler.count("insert helpMerge")
            help(curr, currState); restarts = 0; curr = head }
        case Delete(pred) =>
          // delCount += 1
          if(currState.next != null) curr = currState.next
          else if(restarts < RestartLimit || random.nextInt(4) != 0)
            restart()
            // last node is being deleted; restart
          else{
            // Profiler.count("insert-helpDelete")
            helpDelete(pred, curr, currState); myAssert(curr.deleted)
            restarts = 0; curr = head  
          }
        case Inserted(next, _) =>
          assert(currState.next == next && next != null)
          curr = next
        case ug: UnionGiver  =>
          assert(curr == head & currState.next != null) 
          // Helping isn't necessary here: labelHead will help. 
          // helpUG(curr, currState, ug); assert(curr.getState != currState)
          curr = currState.next
        case _: HeapLabelList =>
          assert(curr == head)
          if(currState.next != null) curr = currState.next
          else{
            // log("inserting "+key+" after "+curr.toString(1))
            done = insertAtEnd(myNode, curr, currState)
          }
        case _: UnionLast => curr = currState.next; assert(curr != null)
      } // end of match
    }
    //Profiler.count("insert iter", iterCount);
    //Profiler.count("insert restarts", restartCount)
    maybeTidy()
  }

  @inline private def insertSpin(maxDelay: Int) =
    ox.cads.util.NanoSpin(random.nextInt(maxDelay))

  // ======== insertBelow

  /** Try to insert myNode below parent with state parentState.  This is the
    * main part of the insertion, whether or not concurrent unions are
    * allowed. */
  private def insertBelow0(myNode: Node, parent: Node, parentState: NodeState)
      : Boolean = {
    val newParentState = new NodeState(
      parent = null, degree = 1, children = List(myNode),
      next = parentState.next, seq = parentState.seq, label = null)
    parent.getState == parentState &&
      parent.compareAndSet(parentState, newParentState)
  }

  /** Try to insert myNode below parent with state parentState.
    * Pre: parent.key <= curr.key, parent singleton root. */
  private def insertBelow(myNode: Node, parent: Node, parentState: NodeState)
      : Boolean = {
    // myNode.setParentInitial(parent)
    if(allowConcurrentUnion){
      // log(": insertBelow("+id+", "+myNode.key+
      //       "); parent = "+parent.toString(1))
      val ep = epoch.get
      // Add InsertBelow label to head and set heapIndirection
      if(parent.getHeap == this){
        myNode.heapIndirection.set(heapIndirection.get)
        val label = InsertBelow(myNode, parent, parentState, ep)
        if(labelHead(label, parent, parentState, ep) &&
             completeInsertBelow(myNode, parent, parentState, label)){
          // log("insertBelow succeeded"+(parent.getHeap == this));
          true
        }
        else{
          // log("insertBelow failed "+(parent.getHeap == this))
          false // myNode.clear; false
        }
      }
      else false // { log("insertBelow: parent heap changed"); false }
        // maybeHelpUG(pHeap) -- no need
    }
    else // not allowConcurrentUnion
      insertBelow0(myNode, parent, parentState)
  }

  /** Complete the insertion of myNode below parent with state parentState.  The
    * head node label includes label, to prevent concurrent unions.
    * Other threads can help with this part. */
  protected def completeInsertBelow(
    myNode: Node, parent: Node, parentState: NodeState, label: InsertBelow)
      : Boolean = {
    // log("completeInsertBelow: heap = "+id+"; key = "+myNode.key+
    //       "; parent = "+parent.key)
    var ok = false // did this thread complete the insertion?
    // Check parent still in this heap
    if(epoch.get == label.epoch && !label.done){
    // if(parent.getHeap == this && myNode.getHeap == this && !label.done){
      assert(myNode.getHeap == this || label.done)
      // Try to update parent
      if(insertBelow0(myNode, parent, parentState)) ok = true // LP
      myAssert(parent.getHeap == this || label.done,
             ThreadID.get+"parent's heap has changed from "+id+" to "+
               parent.getHeap.id+"; key = "+myNode.key)
    } // end of if(parent.getHeap == this && ...)
    // This operation is complete, possibly unsuccessfully.
    label.done = true; tidyHeapLabelList 
    // Return result
    if(ok) true // { log("completeInsertBelow successful"); true }
    else{ // test if another thread did the CAS
      val newParentChildren = parent.getState.children
      if(newParentChildren.nonEmpty && newParentChildren.last == myNode){
        // Another thread did the CAS, helping
        // log("Another thread helped with insertBelow");
        true
      }
      else false
    }
  }

  // ======== insertAtEnd

  /** Insert at the end of the root list, after last with state lastState. */
  private def insertAtEnd(myNode: Node, last: Node, lastState: NodeState)
      : Boolean = {
    assert(lastState.next == null && lastState.parent == null)
    assert(lastState.label == null ||
             last == head && lastState.label.isInstanceOf[HeapLabelList])
    // log(ThreadID.get+": insertAtEnd("+id+", "+myNode.key+
    //           "); last = "+last.toString(1))
    // Note that if last = head, there is no need for this labelling.
    if(allowConcurrentUnion && last != head){
      val ep = epoch.get
      if(last.getHeap == this){
        // Set heapIndirection in myNode 
        myNode.heapIndirection.set(heapIndirection.get)
        val label = InsertAtEnd(myNode, last, lastState, ep)
        // attempt to add label to head, then attempt to complete the
        // insertion.
        if(labelHead(label, last, lastState, ep) &&
             completeInsertAtEnd(myNode, last, lastState, label)){
          // log("insertAtEnd succeeded; "+(last.getHeap == this))
          true
        }
        else{ /* log("insertAtEnd failed; "+(last.getHeap == this)); */ false }
      }
      else false // { log("insertAtEnd: last changed heap"); false }
        // maybeHelpUG(lHeap) -- no need
    }
    else{
      if(allowConcurrentUnion)  // so last = head
        myNode.heapIndirection.set(heapIndirection.get)
      last.getState == lastState &&
        last.compareAndSet(lastState, lastState.setNext(myNode))
    }
  }

  /** Complete the insertion of myNode after last, with state lastState, and
    * remove label from the head.  Other threads may help with this part as a
    * result of ia*/
  protected def completeInsertAtEnd(
    myNode: Node, last: Node, lastState: NodeState, ia: InsertAtEnd)
      : Boolean = {
    // Check last still in heap
    if(epoch.get == ia.epoch && !ia.done){
      // val myNodeHi = myNode.heapIndirection.get.toString
      // val lastHi = last.heapIndirection.get.toString
      // val heapHi = heapIndirection.get.toString
      // val lastHeap = last.getHeap; val myNodeHeap = myNode.getHeap
      // myAssert(lastHeap == this && myNodeHeap == this || ia.done,
      //          "lastHeap = "+lastHeap.id+"; myNodeHeap = "+myNodeHeap.id+
      //            "; this = "+id+"\nlast = "+last+
      //            "\nlastState = "+lastState.toString(3,3,last)+
      //            "\n"+(epoch.get, ia.epoch, ia.done)+"\n"+
      //            myNodeHi+"\n"+lastHi+"\n"+heapHi)
      // Now try to update last.  There have been no concurrent unions since
      // the labelling, so last has been in this heap since then.  All helping
      // threads will take this branch until ia is marked as done.  Either (1)
      // the first thread will succeed; other threads will detect success via
      // testIfInserted; one of them will set ia as succeeded; or (2) all
      // threads will fail in the CAS, set ia as failed, and return false (via
      // testIfInserted).
      val lastStateL =
        new NodeState(
          degree = lastState.degree, children = lastState.children,
          next = myNode, seq = lastState.seq, label = Inserted(myNode, ia))
      if(last.getState == lastState &&
           last.compareAndSet(lastState, lastStateL)){
        // log("completeInsertAtEnd, CAS done; last = "+last.toString(3)+
        //       "; myNode.getHeap = "+myNode.getHeap.id+"; this = "+id)
        clearInserted(myNode, last, lastStateL, ia); true
      }
      else{
        val lastHeap = last.getHeap; val myNodeHeap = myNode.getHeap
        myAssert(lastHeap == this && myNodeHeap == this || ia.done,
                 "lastHeap = "+lastHeap.id+"; myNodeHeap = "+myNodeHeap.id+
                   "; this = "+id+"\nlast = "+last+
                   "\n"+(epoch.get, ia.epoch, ia.done))
        testIfInserted(myNode, last, ia)
      }
    } // end of if(epoch.get == ia.epoch && !ia.done)
    else{
      // log("completeInsertAtEnd: last no longer in heap or insertion done")
      testIfInserted(myNode, last, ia)
    }
  }

  /** Test if another node has done the inserting CAS, to insert myNode after
    * last, corresponding to status label.  If so, carry on, removing the
    * Inserted label if necessary, and setting ia as successful. */
  protected def testIfInserted(myNode: Node, last: Node, ia: InsertAtEnd)
      : Boolean = {
    if(ia.done){ /* log("testIfInserted "+ia.success); */ ia.success }
    else{
      val lastState = last.getState
      lastState.label match{
        case l @ Inserted(n,_) if n == myNode =>
          // log("completeInsertAtEnd inserting CAS done by another thread")
          clearInserted(myNode, last, lastState, ia)
          assert((last.getLabel ne l) && ia.done)
          true
        case _ => // Note: success field is set before label is removed.
          // if(ia.success) log("completeInsertAtEnd helped by another thread")
          // else log("completeInsertAtEnd failed")
          ia.done = true; ia.success
      } // end of match
    }
  }

  /** Update ia to indicate that myNode has been successfully inserted at the
    * end of the list after last; remove the Inserted label from last (from
    * state lastStateL); mark label ia is done.  Other threads may help with
    * this as a result of the Inserted label. */
  protected def clearInserted(
    myNode: Node, last: Node, lastStateL: NodeState, ia: InsertAtEnd)
    = {
    myAssert(last.getHeap == myNode.getHeap || ia.done,
           "last.getHeap = "+last.getHeap.id+
             "; myNode.getHeap = "+myNode.getHeap.id+
             "; this = "+id+"; ia.done = "+ia.done)
    // It's possible that last.getHeap != this, if the current thread was
    // working in the giving heap of a union that has just happened.
    ia.success = true
    if(last.getState == lastStateL)
      last.compareAndSet(lastStateL, lastStateL.addLabel(null))
    ia.done = true; tidyHeapLabelList
  }

  // ========= findLast

  /** The last node in the heap and its state.  This function helps with any
    * operation except a UnionReceiver on that last node. */
  protected def findLast : (Node, NodeState) = {
    var curr = head; var currState : NodeState = null // current node, state
    var done = false
    // val Limit = 100000; var iters = 0 // # iterations, and limit

    // Try to skip over the next node if it is being merged.
    @inline def skip(t1: Node, t2: Node, state2: NodeState) = {
      val next = skipOverMerge(t1, t2, state2)
      if(next == null) help(curr, currState) // curr is last root
      else curr = next
    }

    while(!done){
      currState = curr.maybeClearParent()
      // iters += 1;
      // if(iters >= Limit-20)
      //   log(ThreadID.get+": findLast:\n"+this.toString+"\n"+curr.toString(1)+"\n")
      // myAssert(iters < Limit, "findLast iters")
      if(currState.parent != null) curr = currState.parent
      else currState.label match{
        case MergeNext(t1, t2, state2) => skip(t1, t2, state2)
        case MergeParent(pred, _, _, t2, state2) if curr == pred =>
          skip(curr, t2, state2)
        case Delete(pred) =>
          if(currState.next != null) curr = currState.next
          else{
            helpDelete(pred, curr, currState)
            val ch = curr.getState.children
            if(ch.nonEmpty) curr = ch.last else curr = head
          }
        case label =>
          if(currState.next != null) curr = currState.next
          else if(label == null || label.isInstanceOf[HeapLabelList])
            done = true
          else // It seems very rare for the label not to be a MergeParent
            help(curr, currState)
      }
    }
    (curr, currState)
  }
  
}
