package com.cop4520.test3;

import java.io.IOException;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@WebServlet(urlPatterns = "/testingAsyncServlet", asyncSupported = true)
public class TestingAsyncServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	  @Override
	  protected void doGet(HttpServletRequest request, 
	      HttpServletResponse response) throws ServletException, IOException {

	    /*AsyncContext asyncContext = request.startAsync(request, response);
	    asyncContext.setTimeout(10 * 60 * 1000);
	    Dispatcher.addRemoteClient(new RemoteClient(asyncContext));*/

	  }
}
