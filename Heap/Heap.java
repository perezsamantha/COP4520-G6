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
