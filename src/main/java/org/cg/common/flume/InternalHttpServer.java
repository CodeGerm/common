/**
 * 
 */
package org.cg.common.flume;

import java.io.IOException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.ResponderServlet;
import org.apache.avro.ipc.Server;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

/* 
 * Http base embeded server for avro communication. Fork code from internal avro server, add configuration support
 * 
 * @author yanlinwang
 *
 */

/** An HTTP-based Avro RPC {@link Server}. */
public class InternalHttpServer implements Server {
	private org.mortbay.jetty.Server server;

	/** Constructs a server to run on the named port. */
	public InternalHttpServer(Responder responder, int port, int httpConnections)
			throws IOException {
		this(new HealthAwareResponderServlet(responder), null, port, httpConnections);
	}

	/** Constructs a server to run on the named port. */
	public InternalHttpServer(ResponderServlet servlet, int port,
			int httpConnections) throws IOException {
		this(servlet, null, port, httpConnections);
	}

	/** Constructs a server to run on the named port on the specified address. */
	public InternalHttpServer(Responder responder, String bindAddress,
			int port, int httpConnections) throws IOException {
		this(new HealthAwareResponderServlet(responder), bindAddress, port,
				httpConnections);
	}

	/** Constructs a server to run on the named port on the specified address. */
	public InternalHttpServer(ResponderServlet servlet, String bindAddress,
			int port, int httpConnections) throws IOException {
		this.server = new org.mortbay.jetty.Server();
		SelectChannelConnector connector = new SelectChannelConnector();
		connector.setLowResourceMaxIdleTime(10000);
		connector.setAcceptQueueSize(128);
		connector.setResolveNames(false);
		connector.setUseDirectBuffers(false);
		connector.setAcceptors(httpConnections);
		
		if (bindAddress != null) {
			connector.setHost(bindAddress);
		}
		connector.setPort(port);
		server.addConnector(connector);
		new Context(server, "/").addServlet(new ServletHolder(servlet), "/*");
	}

	/** Constructs a server to run with the given connector. */
	public InternalHttpServer(Responder responder, Connector connector)
			throws IOException {
		this(new HealthAwareResponderServlet(responder), connector);
	}

	/** Constructs a server to run with the given connector. */
	public InternalHttpServer(ResponderServlet servlet, Connector connector)
			throws IOException {
		this.server = new org.mortbay.jetty.Server();
		server.addConnector(connector);
		new Context(server, "/").addServlet(new ServletHolder(servlet), "/*");
	}

	public void addConnector(Connector connector) {
		server.addConnector(connector);
	}

	public int getPort() {
		return server.getConnectors()[0].getLocalPort();
	}

	public void close() {
		try {
			server.stop();
		} catch (Exception e) {
			throw new AvroRuntimeException(e);
		}
	}

	/**
	 * Start the server.
	 * 
	 * @throws AvroRuntimeException
	 *             if the underlying Jetty server throws any exception while
	 *             starting.
	 */

	public void start() {
		try {
			server.start();
		} catch (Exception e) {
			throw new AvroRuntimeException(e);
		}
	}

	public void join() throws InterruptedException {
		server.join();
	}
}
