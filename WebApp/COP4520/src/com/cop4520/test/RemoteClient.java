package com.cop4520.test;

import jakarta.servlet.AsyncContext;

public class RemoteClient {

	  private int sent;
	  private final AsyncContext asyncContext;

	  public RemoteClient(AsyncContext asyncContext) {
	    this.asyncContext = asyncContext;
	  }
	  

	  public int get() {
	    return sent;
	  }
	  
	  public AsyncContext getAC() {
		    return asyncContext;
	  }

	  public void increment() {
	    this.sent += 1000;
	  }

}
