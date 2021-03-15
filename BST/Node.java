//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;

public class Node {
	
	// dummy values & flag/tag values
	public static final int DUMMY1 = Integer.MAX_VALUE;
	public static final int DUMMY2 = Integer.MAX_VALUE;
	
	public int key;
	
	public AtomicStampedReference<Node> left;
	public AtomicStampedReference<Node> right;
	
	public static final int INIT = 0;
	public static final int FLAG = 1;
	public static final int TAG = 2;
	
	// leaf constructor
	public Node(int key) {
		this.key = key;
		this.left = new AtomicStampedReference<Node>(null, INIT);;
		this.right = new AtomicStampedReference<Node>(null, INIT);;
	}
	
	public Node(int key, AtomicStampedReference<Node> left, AtomicStampedReference<Node> right) {
		this.key = key;
		this.left = left;
		this.right = right;
	}
}
