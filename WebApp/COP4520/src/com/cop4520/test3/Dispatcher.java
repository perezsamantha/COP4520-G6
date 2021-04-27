package com.cop4520.test3;

import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.annotation.WebListener;

@WebListener
public class Dispatcher implements ServletContextListener {

  private static int KEY = 1;
  private static final int PROCESSING_THREAD_COUNT = 3;
  private static final ParallelVEB 
           REMOTE_CLIENTS = new ParallelVEB(100);
  private final Executor executor = Executors.newFixedThreadPool(PROCESSING_THREAD_COUNT);

  public static void addRemoteClient(RemoteClient remoteClient) {
	  REMOTE_CLIENTS.insert(remoteClient, KEY++);
  }


  @Override
  public void contextInitialized(ServletContextEvent event) {
    int count = 0;
    while (count < PROCESSING_THREAD_COUNT) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          while (true) {

            RemoteClient remoteClient;
            // fetch a remote client from the waiting queue
              // (this call blocks until a client is available)
             Node n = REMOTE_CLIENTS.popMin();
             remoteClient = n.remoteClient;

            AsyncContext asyncContext = remoteClient.getAsyncContext();
            ServletResponse response = asyncContext.getResponse();
            response.setContentType("text/plain");

            try {
              Thread.sleep(2000);
            } catch (InterruptedException e1) {
              throw new RuntimeException(e1);
            }

            // increment bytes sent by 10
            remoteClient.incrementBytesSent();

            try {
              // send bytes to client
              PrintWriter out = response.getWriter();
              out.print("Already sent " + remoteClient.getBytesSent() + " bytes\n");
              out.flush();

              // check if we have already sent the 100 bytes to this client
              if (remoteClient.getBytesSent() < 100) {
                // if not, put the client again in the queue
                REMOTE_CLIENTS.insert(remoteClient, KEY++);
              } else {
                // if the 100 bytes are sent, the response is complete
                asyncContext.complete();
              }

            } catch (Exception e) {
              // discard current client
              asyncContext.complete();
            }
          }
        }
      });
      count++;
    }
  }

  @Override
  public void contextDestroyed(ServletContextEvent event) {
  }

}
