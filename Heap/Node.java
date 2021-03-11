import java.util.concurrent.atomic.*;

public class NodeState{
	
	public Node parent;
	public int degree;
	public List<Node> children;
	public Node next;
	public int seq;
	public String label;

	public NodeState(Node parent, int degree, List<Node> children,
					 Node next, int seq, String label){
		this.parent = parent;
		this.degree = degree;
		this.children = new List<>();
		this.next = next;
		this.seq = seq;
		this.label = label;
	}

	public NodeState(int degree, List<Node> children, Node next, int seq){
		this.parent = null;
		this.degree = degree;
		this.children = new List<>();
		this.next = next;
		this.seq = seq;
		this.label = null;
	}

	public void isUnlabelled(){
		return this.label == null;
	}

	public void isParentless(){
		return this.parent == null;
	}

	public void isUnlabelledParentless(){
		return (this.label == null ) && (this.parent == null);
	}

	public NodeState setNext(Node newNext){
		new NodeState(this.parent, this.degree, this.children, newNext, this.seq, this.label);
	}

	public NodeState addLabel(String newLabel){
		new NodeState(this.parent, this.degree, this.children, this.next, this.seq, newLabel);
	}

	public NodeState clearParent(){
		new NodeState(null, this.degree, this.children, this.next, this.seq, newNext);
	}

	public boolean isRoot(){
		return (this.parent == null) || (parent.label == "deleted");
	}


}	

public class Node{

	public int key;
	private AtomicReference<NodeState> state;

	public Node(int key, Node parent, int degree, List<Node> children,
					 Node next, int seq, String label){
		this.key = key;
		state = new AtomicReference<NodeState>(parent, degree, children,
					next, seq,label);

	}

	public Node(int key, int degree, List<Node> children, Node next, int seq){
		this.key = key;
		state = new AtomicReference<NodeState>(degree, children, next, seq);
	}

	public NodeState getState(){
		return state;
	}

	public boolean compareAndSet(NodeState oldState, NodeState newState){
		return state.compareAndSet(oldState, newState);
	}

}