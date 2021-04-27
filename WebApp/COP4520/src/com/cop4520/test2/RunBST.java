package com.cop4520.test2;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.servlet.ServletContextListener;
import jakarta.servlet.annotation.WebListener;

@WebListener
public class RunBST implements ServletContextListener {
	
	public static AtomicReference<SeekRecord> shared = new AtomicReference<>();
	private static int i;
	private static final int numThreads = 8;
	private static final int capacity = 100;
	
	// instantiate BST
	private static final BST REMOTE_CLIENTS = new BST();
	
	// instantiate array of threads
	private static final TestThread[] threads = new TestThread[numThreads];
	
	// atomic int to concurrently manage keys for priority
	private static AtomicInteger key = new AtomicInteger();
	
	// atomic int to concurrently manage nodes removed
	private static AtomicInteger popped = new AtomicInteger();
	
	
	public static void addRemoteClient(RemoteClient remoteClient) {
		
		key.getAndIncrement();
		
		popped.getAndIncrement();
		
		// begin execution time
		long startTime = System.currentTimeMillis();
		
		// start threads
		for (i = 0; i < numThreads; i++) {
			threads[i] = new TestThread(REMOTE_CLIENTS, remoteClient, key, popped, capacity, i + 1);
			threads[i].setName("Thread #" + (i + 1));
	    	threads[i].start();
		}
		
		// join threads
		for (i = 0; i < numThreads; i++) {
	    	try {
				threads[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
		
		// calculate total execution time
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		
		System.out.println();
		System.out.println("Total Execution Time: " + (totalTime / 1000) + " seconds.");

	}

}

