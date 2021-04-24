import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class TestThread extends Thread{
	
	BST tree;
	AtomicInteger key, popped;
	
	int capacity, currValue, currKey, ID;
	
	public TestThread (BST tree, AtomicInteger key, AtomicInteger popped, int capacity, int ID) {
		this.tree = tree;
		this.key = key;
		this.popped = popped;
		this.capacity = capacity;
		this.ID = ID;
	}
	
	public void run () {
		
		while (true) {
			// break if all keys have been inserted
			if (key.get() > capacity) {
				break;
			}
			
			// randomize integer value to be inserted into tree
			currValue = ThreadLocalRandom.current().nextInt(0, capacity);
			
			// get next key
			currKey = key.getAndIncrement();
			
			// extra check
			if (currKey > capacity) {
				break;
			}
			
			// keep trying to insert
			while (!tree.insert(currKey, currValue)) {
				//Thread.yield();
			}
			
			Thread.yield();
			
		}

		// only run <= 4 threads for efficiency purposes
		if (ID <= 4) {
			while (true) {
				// delete node with prioritized key from tree
				Node pop = tree.popMin();
				
				// check if at dummy nodes
				if (pop != null) {
					popped.getAndIncrement();
					if (pop.key >= Node.DUMMY3 && popped.get() > capacity + 1) {
						break;
					}
				}
			
				Thread.yield();
			} 
		}
				
	}

}
