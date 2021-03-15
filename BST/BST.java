import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;

public class BST {
	
	private AtomicStampedReference<Node> root;
	
	//public AtomicReference<SeekRecord> sr;
	public SeekRecord sr;
	
	//public Node parentField;
	//public Node currentField;
	public Node current;
	
	public AtomicStampedReference<Node> parentField;
	public AtomicStampedReference<Node> currentField;
	//public AtomicStampedReference<Node> current;
	
	public AtomicStampedReference<Node> parent;
	public AtomicStampedReference<Node> leaf;
	public AtomicStampedReference<Node> child;
	public AtomicStampedReference<Node> sibling;
	
	AtomicInteger mode;
	
	public BST () { // initialize tree
		AtomicStampedReference<Node> initLeft = new AtomicStampedReference<Node>(new Node(Node.DUMMY2), 0);
		AtomicStampedReference<Node> initRight = new AtomicStampedReference<Node>(new Node(Node.DUMMY1), 0);
		root = new AtomicStampedReference<Node>(new Node(Node.DUMMY1, initLeft, initRight), 0);
		//sr = new AtomicReference<SeekRecord>(new SeekRecord());
		mode = new AtomicInteger();
	}
	
	public SeekRecord seek(int key) {
		sr = new SeekRecord(); // change to instantiate in constructor ?
		//sr = new AtomicReference<SeekRecord>();
		
		sr.ancestor = root;
		sr.successor = root.getReference().left;
		sr.parent = root.getReference().left;
		sr.leaf = root.getReference().left.getReference().left;
		
		// initialize other variables used in traversal
		parentField = sr.parent.getReference().left;
		currentField = sr.leaf.getReference().left;
		current = currentField.getReference();
		
		while (current != null) {
			// move down the tree
			// check if edge from current parent node in access path is tagged
			if (parentField.getStamp() != 2) {
				// found untagged edge in access path, advance ancestor and successor pointers
				sr.ancestor = sr.parent;
				sr.successor = sr.leaf;
			}
			// advance parent and leaf pointers
			sr.parent = sr.leaf;
			//sr.leaf = current;
			sr.leaf.set(current, sr.leaf.getStamp());
			
			// update other variables used in traversal
			parentField = currentField;
			if (key < current.key) {
				currentField = current.left;
			} else {
				currentField = current.right;
			}
			
			current = currentField.getReference();
		}
		
		// traversal complete
		return sr; // convert to void method?
		
	}
	
	public Boolean search(int key) {
		seek(key);
		if (sr.leaf.getReference().key == key) { // key present in tree
			return true;
		} else { // key not present in tree
			return false;
		}
	}
	
	public Boolean insert(int key) {
		while (true) {
			seek(key);
			if (sr.leaf.getReference().key != key) {
				// key not present in tree
				parent = sr.parent;
				leaf = sr.leaf;
				
				// obtain address of child field that needs to be modified
				if (key < parent.getReference().key) {
					child = parent.getReference().left;
				} else {
					child = parent.getReference().right;
				}
				
				// create two nodes newInternal and newLeaf and initialize them appropriately
				AtomicStampedReference<Node> newLeaf = new AtomicStampedReference<Node>(new Node(key), 0);
				AtomicStampedReference<Node> newInternal;
				
				int currKey = leaf.getReference().key;
				if (key < currKey) {
					newInternal = new AtomicStampedReference<Node>(new Node(currKey, newLeaf, leaf), 0);
				} else {
					newInternal = new AtomicStampedReference<Node>(new Node(key, leaf, newLeaf), 0);
				}
				
				
				// try to add the new nodes to the tree
				Boolean result = child.compareAndSet(leaf.getReference(), newInternal.getReference(), 0, 0); // stamp should be 0
				
				if (result) {
					return true;
				} else {
					// insertion failed; help conflicting delete op
					if (child.getReference() == leaf.getReference() && (child.getStamp() == 1 || child.getStamp() == 2)) {
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
		// start in injection mode
		mode.set(1);
		
		while (true) {
			seek(key);
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
				Boolean result = child.compareAndSet(leaf.getReference(), leaf.getReference(), leaf.getStamp() + 1, 1);
				
				if (result) {
					// advance to cleanup mode and try to remove the leaf node from the tree
					mode.set(2);
					Boolean done = cleanup(key, sr);
					if (done) {
						return true;
					}
				} else {
					//
					if (child.getReference() == leaf.getReference() && (child.getStamp() == 1 || child.getStamp() == 2)) {
						// address of child has not changed and either leaf node or sibling has been flagged for deletion
						cleanup(key, sr);
					}
				}
			} else {
				// cleanup mode; check if the leaf node that was flagged in injection mode is still present in the tree
				if (sr.leaf != leaf) {
					// leaf node no longer present in tree
					return true;
				} else {
					// leaf node is still present in the tree; remove it
					Boolean done = cleanup(key, sr);
					if (done) {
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
		//AtomicStampedReference<Node> leaf = csr.leaf;
		
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
		sibling.set(sibling.getReference(), 2);
		
		// 
		Boolean result = successor.compareAndSet(successor.getReference(), sibling.getReference(), sibling.getStamp(), 2);
				
		return result; 
	}
	
}
