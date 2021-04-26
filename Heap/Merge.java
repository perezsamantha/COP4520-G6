import java.util.concurrent.atomic.*;

public void merge(Node a, NodeState aState, Node pred, NodeState predState, Node b, NodeState bState){
	if(!(a != b && a.key <= b.key && aState.degree == bState.degree && pState.next == b && aState.unlabelled && aState.parentless && bState.unlabelled && bState.parentless && pState.unlabelled && pState.parentless))
		return -1;

	if(a != pred)
		String pLabel = mergeNext(a, b, bState)
	else
		String pLabel = null;

	// aStateL translation here
	
	a.compareAndSet(aState, aStateL)
}


//To Translate

/** Help with merger of non-successive trees by labelling pred.  t1 has been
    * labelled giving state1L.  pLabel needs to be added to pred.  Other
    * arguments as for merge. */
  protected def mergeLabelPred(
    t1: Node, state1L: NodeState, pred: Node, pState: NodeState,
    pLabel: MergeNext, t2: Node, state2: NodeState) : Boolean
    = {
    myAssert(t1.key <= t2.key && t1 != pred && state1L.degree == state2.degree &&
               pState.next == t2 && state1L.parent == null &&
               state1L.label.isInstanceOf[MergeParent] &&
               state2.unlabelledParentless && pState.unlabelledParentless)
    val pStateL = pState.addLabel(pLabel)
    if(pred.getState == pState && t2.getState == state2 &&
         pred.compareAndSet(pState, pStateL))
      mergeUpdateT2(t1, state1L, pred, pStateL, t2, state2) 
    else{
      if(t1.getState == state1L && (pred.getState.label ne pLabel) &&
           t2.getState.parent != t1)
        // Either pred labelling hasn't happened; or pred update has happened,
        // and t2 has been subsequently updated, meaning that the t1 update
        // has happened.  Put another way: the second conjunct ensures we're
        // not between the pred label and pred update steps; the thrid
        // conjunct ensures we're not between the t2 update and t1 update
        // steps.  Remove label from t1
        if(!t1.compareAndSet(state1L, state1L.addLabel(null)))
          // someone else removed label
          myAssert(! t1.getState.label.eq(state1L.label))
      false
    }
  }

  /** Help with merger of non-successive trees by performing t2 update.  t1 has
    * been labelled.  If t1 != pred then pred has been labelled; if t1 ==
    * pred, then the value of pStateL is irrelevant. */
  protected def mergeUpdateT2(
    t1: Node, state1L: NodeState, pred: Node, pStateL: NodeState,
    t2: Node, state2: NodeState)
      : Boolean = {
    myAssert(t1.key <= t2.key && state1L.degree == state2.degree &&
               state1L.parent == null && state2.unlabelledParentless &&
               state1L.label.isInstanceOf[MergeParent],
             "t1 = "+t1+"\npred = "+pred+"\n"+"pStateL = "+
               pStateL.toString(1,1,pred)+(t1==pred))
    myAssert(t1 == pred ||
               pStateL.next == t2 && pStateL.parent == null &&
               pStateL.label.isInstanceOf[MergeNext])
    val newNext = if(state1L.children.isEmpty) null else state1L.children.head
    val state2U = new NodeState(
      parent = t1, degree = state2.degree, 
      children = state2.children, next = newNext, seq = state2.seq+1)
    if(t2.getState == state2 && t2.compareAndSet(state2, state2U)){
      // rootCount.getAndDecrement
      if(t1 != pred)
        mergeUpdatePred(t1, state1L, pred, pStateL, t2, state2.next)
      else mergeUpdateT1Pred(t1, state1L, t2, state2.next)
      true // merge bound to succeed
    }
    else{
      if(t2.getState.parent != t1){
        // Either t2 has been updated in an interfering way, or the update on
        // t1 (and hence the whole operation) has completed, and some later
        // update has acted on t2.  Remove labels from pred and t1; the order
        // doesn't matter, here.
        if(t1 != pred){
          if(pred.getState == pStateL &&
               pred.compareAndSet(pStateL, pStateL.addLabel(null))){ }
          else myAssert(pred.getState.label ne pStateL.label)
        }
        if(t1.getState == state1L)
          t1.compareAndSet(state1L, state1L.addLabel(null))
        else myAssert(t1.getState.label ne state1L.label)
      }
      false
    } // end of outer else
  }

  /** Complete the merger, updating pred and then t1.  
    * Pre: the t2 update has happened.*/
  protected def mergeUpdatePred(
    t1: Node, state1L: NodeState, pred: Node, pStateL: NodeState,
    t2: Node, nextNode: Node) = {
    myAssert(t1.key <= t2.key && pStateL.next == t2 && state1L.parent == null &&
               state1L.label.isInstanceOf[MergeParent] &&
               pStateL.parent == null && pStateL.label.isInstanceOf[MergeNext])
    myAssert(t2.getState.parent == t1 ||
               t1.getState != state1L && pred.getState != pStateL)
    // update pred
    val pStateU = new NodeState(
      parent = null, degree = pStateL.degree, 
      children = pStateL.children, next = nextNode, seq = pStateL.seq)
    if(pred.getState == pStateL && pred.compareAndSet(pStateL, pStateU))
      mergeUpdateT1(t1, state1L, pred, t2)
  }

  /** Complete the merger, updating t1. */
  protected def mergeUpdateT1(t1: Node, state1L: NodeState, pred: Node, t2: Node)
    = {
    val state1U = new NodeState(
      degree = state1L.degree+1, children = t2 :: state1L.children, 
      next = state1L.next, seq = state1L.seq)
    if(t1.getState == state1L) t1.compareAndSet(state1L, state1U)
    // else another thread completed merger. 
  }

  /** Complete the merger of adjacent trees, updating t1. */
  protected def mergeUpdateT1Pred(
    t1: Node, state1L: NodeState, t2: Node, nextNode: Node)
    = {
    val state1U = new NodeState(
      degree = state1L.degree+1, children = t2 :: state1L.children,
      next = nextNode, seq = state1L.seq)
    if(t1.getState == state1L) t1.compareAndSet(state1L, state1U)
  }



}
