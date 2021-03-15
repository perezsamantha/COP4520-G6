import java.util.concurrent.atomic.AtomicStampedReference;

public class SeekRecord {
	
	public AtomicStampedReference<Node> ancestor;
	public AtomicStampedReference<Node> successor;
	public AtomicStampedReference<Node> parent;
	public AtomicStampedReference<Node> leaf;
	
	public SeekRecord() {
		this.ancestor = new AtomicStampedReference<Node>(null, 0);
		this.successor = new AtomicStampedReference<Node>(null, 0);
		this.parent = new AtomicStampedReference<Node>(null, 0);
		this.leaf = new AtomicStampedReference<Node>(null, 0);
	}
}
