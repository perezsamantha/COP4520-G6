import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicStampedReference;

public class BST {
	
	public AtomicStampedReference<Node> root;
	
	AtomicInteger mode;
	
	public BST () { // initialize tree
		AtomicStampedReference<Node> initLeft = new AtomicStampedReference<Node>(new Node(Node.DUMMY2, -1), 0);
		AtomicStampedReference<Node> initRight = new AtomicStampedReference<Node>(new Node(Node.DUMMY1, -1), 0);
		root = new AtomicStampedReference<Node>(new Node(Node.DUMMY1, -1, initLeft, initRight), 0);
		root.getReference().left.getReference().left = new AtomicStampedReference<Node>(new Node (Node.DUMMY3, -1), 0);
		mode = new AtomicInteger();
	}
	
	public SeekRecord seek(int key) {
		SeekRecord sr = new SeekRecord();
		
		sr.ancestor = root;
		sr.successor = root.getReference().left;
		sr.parent = root.getReference().left;
		sr.leaf = root.getReference().left.getReference().left;

		
		// initialize other variables used in traversal
		AtomicStampedReference<Node> parentField = sr.parent.getReference().left;
		AtomicStampedReference<Node> currentField = sr.leaf.getReference().left;
		AtomicStampedReference<Node> current = currentField;
		
		while (current.getReference() != null) {

			// move down the tree
			// check if edge from current parent node in access path is tagged
			if (parentField.getStamp() != 2) {
				// found untagged edge in access path, advance ancestor and successor pointers
				sr.ancestor = sr.parent;
				sr.successor = sr.leaf;
			}
			// advance parent and leaf pointers
			sr.parent = sr.leaf;
			sr.leaf = current;
			
			// update other variables used in traversal
			parentField = currentField;
			
			if (key < current.getReference().key) {
				currentField = current.getReference().left;
			} else {
				currentField = current.getReference().right;
			}
			
			current = currentField;
		}
		
		// traversal complete
		return sr; 
		
	}
	
	public Boolean search(int key) {
		SeekRecord sr = seek(key);
		if (sr.leaf.getReference().key == key) { // key present in tree
			return true;
		} else { // key not present in tree
			return false;
		}
	}
	
	public Boolean insert(int key, int value) {
		while (true) {
			SeekRecord sr = seek(key);
			if (sr.leaf.getReference().key != key) {
				// key not present in tree
				AtomicStampedReference<Node> parent = sr.parent;
				AtomicStampedReference<Node> leaf = sr.leaf;
				AtomicStampedReference<Node> child;
				
				// obtain address of child field that needs to be modified
				if (key < parent.getReference().key) {
					child = parent.getReference().left;
				} else {
					child = parent.getReference().right;
				}
				
				// create two nodes newInternal and newLeaf and initialize them appropriately
				AtomicStampedReference<Node> newLeaf = new AtomicStampedReference<Node>(new Node(key, value), 0);
				AtomicStampedReference<Node> newInternal;
				
				Node currLeaf = leaf.getReference();
				int currKey = currLeaf.key;
				int currValue = currLeaf.value;
				AtomicStampedReference<Node> newSibling = new AtomicStampedReference<Node>(new Node(currKey, currValue), 0);

				if (key < currKey) {
					newInternal = new AtomicStampedReference<Node>(new Node(currKey, currValue, newLeaf, newSibling), 0);
				} else {
					newInternal = new AtomicStampedReference<Node>(new Node(key, value, newSibling, newLeaf), 0);
				}
				
				// try to add the new nodes to the tree
				Boolean result = child.compareAndSet(leaf.getReference(), newInternal.getReference(), 0, 0); // stamp should be 0
				
				if (result) {
					//System.out.printf("%10s - NODE: %6d   VALUE: %6d\n", "INSERTED", key, value);
					return true;
				} else {
					// insertion failed; help conflicting delete op
					if (child.getReference() == leaf.getReference() && (child.getStamp() > 0)) {
						// address of child has not changed and either leaf node or sibling has been flagged for deletion
						cleanup(key, sr);
					}
				}
			} else {
				// key already present in tree
				return false;
			}
		}
	}
	
	public Boolean delete(int key) {
		AtomicStampedReference<Node> parent;
		AtomicStampedReference<Node> child;
		AtomicStampedReference<Node> leaf;
		
		// start in injection mode
		mode.set(1);
		
		while (true) {
			SeekRecord sr = seek(key);
			parent = sr.parent;
			
			// obtain address of child field that needs to be modified
			if (key < parent.getReference().key) {
				child = parent.getReference().left;
			} else {
				child = parent.getReference().right;
			}
			
			if (mode.get() == 1) {
				// injection mode; check if key is present in tree
				leaf = sr.leaf;
				if (leaf.getReference().key != key) {
					return false; // key not present in tree
				}
				// inject the delete op into the tree
				Boolean result = child.compareAndSet(leaf.getReference(), leaf.getReference(), 0, 1);
				
				if (result) {
					// advance to cleanup mode and try to remove the leaf node from the tree
					mode.set(2);
					Boolean done = cleanup(key, sr);
					if (done) {
						System.out.printf("%10s - NODE: %6d   VALUE: %6d\n", "POPPED", leaf.getReference().key, leaf.getReference().value);
						return true;
					}
				} else {
					//
					if (child.getReference() == leaf.getReference() && (child.getStamp() > 0)) {
						// address of child has not changed and either leaf node or sibling has been flagged for deletion
						cleanup(key, sr);
					}
				}
			} else {
				// cleanup mode; check if the leaf node that was flagged in injection mode is still present in the tree
				if (!search(sr.leaf.getReference().key)) {
					System.out.printf("%10s - NODE: %6d   VALUE: %6d\n", "POPPED", sr.leaf.getReference().key, sr.leaf.getReference().value);
					// leaf node no longer present in tree
					return true;
				} else {
					// leaf node is still present in the tree; remove it
					Boolean done = cleanup(key, sr);
					if (done) {
						System.out.printf("%10s - NODE: %6d   VALUE: %6d\n", "POPPED", sr.leaf.getReference().key, sr.leaf.getReference().value);
						return true;
					}
				}
			}
		}
	}
	
	public Boolean cleanup(int key, SeekRecord csr) {
		// retrieve all addresses stored in seek record for easy access
		AtomicStampedReference<Node> ancestor = csr.ancestor;
		AtomicStampedReference<Node> successor = csr.successor;
		AtomicStampedReference<Node> parent = csr.parent;
		
		AtomicStampedReference<Node> child;
		AtomicStampedReference<Node> sibling;
		
		// obtain all addresses on which atomic instructions will be executed
		// first obtain address of field of ancestor node that will be modified
		if (key < ancestor.getReference().key) {
			successor = ancestor.getReference().left;
		} else {
			successor = ancestor.getReference().right;
		}
		
		// now obtain addresses of child field of parent node
		if (key < parent.getReference().key) {
			child = parent.getReference().left;
			sibling = parent.getReference().right;
		} else {
			child = parent.getReference().right;
			sibling = parent.getReference().left;
		}
		
		// retrieve stamp of child
		if (child.getStamp() != 1) {
			// leaf node is not flagged for deletion; sibling node must be flagged for deletion
			// switch sibling address
			sibling.set(child.getReference(), sibling.getStamp());
		}
		
		// tag the sibling edge; no modify op can occur on this edge now
		sibling.compareAndSet(sibling.getReference(), sibling.getReference(), 0, 2);		
		
		Boolean result;
		
		// there's an issue with a few CAS operations for deletion I'm assuming :/
		// this line might be the problem, I guess stamps weren't the solution :(
		// need to reset tag but copy in flag at same time, stamps only handle one at a time
		result = successor.compareAndSet(successor.getReference(), sibling.getReference(), 0, ((sibling.getStamp() == 1) ? 1 : 0));
		
		return result; 
	}
	
	public synchronized Node popMin() {
		Node current = root.getReference();
		
		while (current.left.getReference() != null) {
			current = current.left.getReference();
		}
		
		if (current.key >= Node.DUMMY3) {
			return current;
		} else if (delete(current.key)) {
			return current;
		} else {
			return null;
		}
	}
	

}
