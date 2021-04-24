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
				
			}
			
			Thread.yield();
			
		}

		// temp solution just to get something working:
		// synchronize popMin() or have only first thread pop
		
		//if (ID == 1) {
		
		while (true) {
			
				// doesn't work perfectly with multiple threads :(
			
				// break if all keys have been popped
				if (popped.get() >= capacity + 1) {
					break;
				}
			
				// delete node with prioritized key from tree
				if (tree.popMin() != null) {
					popped.getAndIncrement();
				}
			
				Thread.yield();
			} 
		//}
			
	}

}
