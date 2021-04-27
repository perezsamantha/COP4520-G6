package com.cop4520.test;

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
public class Manager implements ServletContextListener {

  private static final int threads = 4;
  private final Executor ex = Executors.newFixedThreadPool(threads);
  private static final BlockingQueue<RemoteClient> remoteclient = new LinkedBlockingQueue<RemoteClient>();

  public static void add(RemoteClient remoteClient) {
	  remoteclient.add(remoteClient);
  }


  @Override
  public void contextInitialized(ServletContextEvent event) {
    int i = 0;
    while (i < threads) {
      ex.execute(new Runnable() {
        @Override
        public void run() {
          while (true) {

            RemoteClient rc;
            try {
              // get remote client
            	rc = remoteclient.take();
            } catch (InterruptedException e1) {
              throw new RuntimeException("Interrupted");
            }

            AsyncContext ac = rc.getAC();
            ServletResponse rs = ac.getResponse();
            rs.setContentType("text/plain");

            try {
              Thread.sleep(2000);
            } catch (InterruptedException e1) {
              throw new RuntimeException(e1);
            }

            // increment 
            rc.increment();

            try {
              // send bytes to client
              PrintWriter put = rs.getWriter();
              put.print("Processed" + rc.get() + " bytes\n");
              put.flush();

              // check if  1000 bytes to this client
              if (rc.get() < 1000) {
            	  remoteclient.put(rc);
              } else {
            	  ac.complete();
              }

            } catch (Exception e) {
              // discard
            	ac.complete();
            }
          }
        }
      });
      i++;
    }
  }

  @Override
  public void contextDestroyed(ServletContextEvent event) {
  }

}
