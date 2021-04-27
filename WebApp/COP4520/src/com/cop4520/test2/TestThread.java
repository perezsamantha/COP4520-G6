package com.cop4520.test2;

import java.io.PrintWriter;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.annotation.WebListener;

@WebListener
public class TestThread extends Thread implements ServletContextListener {
	
	BST tree;
	AtomicInteger key, popped;
	
	int capacity, currValue, currKey, ID;
	
    RemoteClient remoteClient;
	
	public TestThread (BST tree, RemoteClient remoteClient, AtomicInteger key, AtomicInteger popped, int capacity, int ID) {
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
			while (!tree.insert(currKey, currValue, remoteClient)) {
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
				
				AsyncContext asyncContext = pop.remoteClient.getAsyncContext();
	            ServletResponse response = asyncContext.getResponse();
	            response.setContentType("text/plain");
	            
	            // increment bytes sent by 10
	            remoteClient.incrementBytesSent();
	            
	            try {
	            // send bytes to client
	            PrintWriter out = response.getWriter();
	            out.print("Already sent " + remoteClient.getBytesSent() + " bytes");
	            out.flush();
	
	            // check if we have already sent the 100 bytes to this client
	            if (remoteClient.getBytesSent() < 100) {
                // if not, put the client again in the queue
	            	tree.insert(pop.key, pop.value, pop.remoteClient);
	            } else {
	            	// if the 100 bytes are sent, the response is complete
	            	asyncContext.complete();
	            }
	
	            } catch (Exception e) {
	              // discard current client
	              asyncContext.complete();
	            }
			
				Thread.yield();
			} 
		}
				
	}

}
