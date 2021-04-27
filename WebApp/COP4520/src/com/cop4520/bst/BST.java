package com.cop4520.bst;
  
import java.util.LinkedList;
import java.util.Queue;
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
			if (parentField.getStamp() != Node.FLAG) {

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
					if (child.getReference() == null) {
						return false;
					} 
				} else {
					child = parent.getReference().right;
					if (child.getReference() == null) {
						return false;
					} 
				}
				
				// create two nodes newInternal and newLeaf and initialize them appropriately
				AtomicStampedReference<Node> newLeaf = new AtomicStampedReference<Node>(new Node(key, value), 0);
				AtomicStampedReference<Node> newInternal;
				
				AtomicStampedReference<Node>  currLeaf = leaf;
				int currKey = currLeaf.getReference().key;
				int currValue = currLeaf.getReference().value;
				
				AtomicStampedReference<Node> newSibling = new AtomicStampedReference<Node>(new Node(currKey, currValue), 0);

				if (key < currKey) {
					newInternal = new AtomicStampedReference<Node>(new Node(currKey, currValue, newLeaf, newSibling), 0);
				} else if (key > currKey) {
					newInternal = new AtomicStampedReference<Node>(new Node(key, value, newSibling, newLeaf), 0);
				} else {
					return false;
				}
				
				// try to add the new nodes to the tree
				Boolean result = child.compareAndSet(leaf.getReference(), newInternal.getReference(), 0, 0);
				
				if (result) {
					//System.out.printf("%10s - NODE: %6d   VALUE: %6d\n", "INSERTED", key, value);
					//printLevelOrder(root.getReference(), depthOfTree(root.getReference(), 1));
					return true;
				} else {
					//child.compareAndSet(child.getReference(), child.getReference(), Node.INSERT, 0); // stamp should be 0

					// insertion failed; help conflicting delete op
					//if (child.getReference() == leaf.getReference() && (child.getStamp() > Node.INSERT)) {
						// address of child has not changed and either leaf node or sibling has been flagged for deletion
						//cleanup(key, sr);
					//}

					return false;
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
				if (child.getReference() == null) {
					return false;
				} 
			} else {
				child = parent.getReference().right;
				if (child.getReference() == null) {
					return false;
				} 
			}
			
			if (mode.get() == 1) {
				// injection mode; check if key is present in tree
				leaf = sr.leaf;
				if (leaf.getReference().key != key) {
					return false; // key not present in tree
				}
				
				// inject the delete op into the tree
				//Boolean result = (leaf.getReference().isLeaf) ? (child.compareAndSet(leaf.getReference(), leaf.getReference(), 0, Node.FLAG)) : false;
				Boolean result = child.compareAndSet(leaf.getReference(), leaf.getReference(), 0, Node.FLAG);
				
				if (result) {
					// advance to cleanup mode and try to remove the leaf node from the tree
					mode.set(2);
					Boolean done = cleanup(key, sr);
					if (done) {
						//System.out.printf("%10s - NODE: %6d   VALUE: %6d\n", "POPPED", leaf.getReference().key, leaf.getReference().value);
						return true;
					}
				} else {
					//
					if (child.getReference() == leaf.getReference() && (child.getStamp() > Node.INSERT)) {
						// address of child has not changed and either leaf node or sibling has been flagged for deletion
						cleanup(key, sr);
					}
				}
			} else {
				// cleanup mode; check if the leaf node that was flagged in injection mode is still present in the tree
				if (!search(sr.leaf.getReference().key)) {
					//System.out.printf("%10s - NODE: %6d   VALUE: %6d\n", "POPPED", sr.leaf.getReference().key, sr.leaf.getReference().value);
					// leaf node no longer present in tree
					return true;
				} else {
					// leaf node is still present in the tree; remove it
					Boolean done = cleanup(key, sr);
					if (done) {
						//System.out.printf("%10s - NODE: %6d   VALUE: %6d\n", "POPPED", sr.leaf.getReference().key, sr.leaf.getReference().value);
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
			if (child.getReference() == null || sibling.getReference() == null) {
				return false;
			} 

		} else {
			child = parent.getReference().right;
			sibling = parent.getReference().left;
			if (child.getReference() == null || sibling.getReference() == null) {
				return false;
			} 
		}
		
		// retrieve stamp of child
		if (child.getStamp() != Node.FLAG) {
			// leaf node is not flagged for deletion; sibling node must be flagged for deletion
			// switch sibling address
			sibling.set(child.getReference(), child.getStamp());
		}
		
		// tag the sibling edge; no modify op can occur on this edge now
		sibling.compareAndSet(sibling.getReference(), sibling.getReference(), 0, Node.TAG);		
		
		Boolean result;
		
		// need to reset tag but copy in flag at same time, stamps only handle one at a time
		result = successor.compareAndSet(successor.getReference(), sibling.getReference(), 0, ((sibling.getStamp() == Node.FLAG) ? Node.FLAG : 0));
		
		if (result) {
			System.out.printf("%10s - NODE: %6d   VALUE: %6d\n", "POPPED", child.getReference().key, child.getReference().value);
		}
		
		return result; 
	}
	
	//synchronized public Node popMin() {
	public Node popMin() {
		Node current = root.getReference();
		
		while (current.left.getReference() != null) {
			current = current.left.getReference();
		}
		
		// if not synchronized, keep this print statement to give best odds
		if (current.key < Node.DUMMY3) {
			System.out.println("Attempting to pop " + current.key);
		}
		
		if (current.key >= Node.DUMMY3) {
			return current;
		} else if (delete(current.key)) {
			return current;
		} else {
			return null;
		}
	}
	
	// methods to print tree for debugging purposes
	
	int depthOfTree(Node root, int d) {
		  if(root == null) {
		    return d;
		  }
		  int left = d;
		  int right = d;
		  if(root.left != null) {
		    left = depthOfTree(root.left.getReference(), d+1);
		  }
		  if(root.right != null) {
		    right = depthOfTree(root.right.getReference(), d+1);
		  }
		  return Math.max(left, right);
		}

	void printLevelOrder(Node root, int depth)
		{
		    if(root == null)
		        return;

		    Queue<Node> q =new LinkedList<Node>();

		    q.add(root);            
		    while(true)
		    {               
		        int nodeCount = q.size();
		        if(nodeCount == 0)
		            break;
		        for(int i=0; i<depth; i++) {
		          System.out.print("  ");
		        }
		        while(nodeCount > 0)
		        {    
		            Node node = q.peek();
		            System.out.print("("+node.key + ")");

		            q.remove();

		            if(node.left.getReference() != null)
		                q.add(node.left.getReference());
		            if(node.right.getReference() != null)
		                q.add(node.right.getReference());

		            if(nodeCount>1){
		               System.out.print(", ");
		            }
		            nodeCount--;    
		        }
		        depth--;
		        System.out.println();
		    }
		} 
	

}

