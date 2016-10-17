package org.cg.common.flume;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.ResponderServlet;

public class HealthAwareResponderServlet extends ResponderServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4198153463629357185L;

	public HealthAwareResponderServlet(Responder responder) throws IOException {
		super(responder);
	}

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		resp.getWriter().print("OK");
	}
}
