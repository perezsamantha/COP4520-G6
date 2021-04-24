import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RunBST {
	
	public static AtomicReference<SeekRecord> shared = new AtomicReference<>();

	public static void main(String[] args) throws InterruptedException {
		int i;
		int numThreads = 8;
		int capacity = 1000;
		
		// instantiate BST
		BST tree = new BST();
		
		// instantiate array of threads
		TestThread[] threads = new TestThread[numThreads];
		
		// atomic int to concurrently manage keys for priority
		AtomicInteger key = new AtomicInteger();
		key.getAndIncrement();
		
		// atomic int to concurrently manage nodes removed
		AtomicInteger popped = new AtomicInteger();
		popped.getAndIncrement();
		
		// begin execution time
		long startTime = System.currentTimeMillis();
		
		// start threads
		for (i = 0; i < numThreads; i++) {
			threads[i] = new TestThread(tree, key, popped, capacity, i + 1);
			threads[i].setName("Thread #" + (i + 1));
	    	threads[i].start();
		}
		
		// join threads
		for (i = 0; i < numThreads; i++) {
	    	threads[i].join();
	    }
		
		// calculate total execution time
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		
		System.out.println();
		System.out.println("Total Execution Time: " + (totalTime / 1000) + " seconds.");

	}

}
