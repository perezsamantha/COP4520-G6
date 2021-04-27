package com.cop4520.test;

import java.io.IOException;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@WebServlet(urlPatterns = "/asyncSimulator", asyncSupported = true)
public class AsyncSimulator extends HttpServlet {

	private static final long serialVersionUID = 1L;

	  @Override
	  protected void doGet(HttpServletRequest request, 
	    HttpServletResponse response) throws ServletException, IOException {

	    AsyncContext asC = request.startAsync(request, response);
	    asC.setTimeout(600000);
	    Manager.add(new RemoteClient(asC));

	  }
}
