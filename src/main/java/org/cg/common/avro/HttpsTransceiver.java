package org.cg.common.avro;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;

import java.net.Proxy;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.avro.ipc.Transceiver;

public class HttpsTransceiver extends Transceiver {

	private SSLSocketFactory factory;
	private HostnameVerifier verifier;

	public HttpsTransceiver(URL url, SSLSocketFactory fac,HostnameVerifier verifer) {
		this(url);
		this.factory = fac;
		this.verifier = verifer;
		// TODO Auto-generated constructor stub
	}

	static final String CONTENT_TYPE = "avro/binary"; 
	  private URL url;
	  private Proxy proxy;
	  private HttpsURLConnection connection;
	  private int timeout;
	  
	  public HttpsTransceiver(URL url) { this.url = url; }

	  public HttpsTransceiver(URL url, Proxy proxy) {
	    this(url);
	    this.proxy = proxy;
	  }

	  /** Set the connect and read timeouts, in milliseconds. */
	  public void setTimeout(int timeout) { this.timeout = timeout; }

	  public String getRemoteName() { return this.url.toString(); }
	    
	  public synchronized List<ByteBuffer> readBuffers() throws IOException {
	    InputStream in = connection.getInputStream();
	    try {
	      return readBuffers(in);
	    } finally {
	      in.close();
	    }
	  }

	  public synchronized void writeBuffers(List<ByteBuffer> buffers)
	    throws IOException {
	    if (proxy == null)
	      connection = (HttpsURLConnection)url.openConnection();
	    else
	      connection = (HttpsURLConnection)url.openConnection(proxy);
	    connection.setHostnameVerifier(verifier);
	    connection.setRequestMethod("POST");
	    connection.setRequestProperty("Content-Type", CONTENT_TYPE);
	    connection.setRequestProperty("Content-Length",
	                                  Integer.toString(getLength(buffers)));
	    connection.setDoOutput(true);
	    connection.setReadTimeout(timeout);
	    connection.setConnectTimeout(timeout);

	    OutputStream out = connection.getOutputStream();
	    try {
	      writeBuffers(buffers, out);
	    } finally {
	      out.close();
	    }
	  }

	  static int getLength(List<ByteBuffer> buffers) {
	    int length = 0;
	    for (ByteBuffer buffer : buffers) {
	      length += 4;
	      length += buffer.remaining();
	    }
	    length += 4;
	    return length;
	  }

	  static List<ByteBuffer> readBuffers(InputStream in)
	    throws IOException {
	    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
	    while (true) {
	      int length = (in.read()<<24)+(in.read()<<16)+(in.read()<<8)+in.read();
	      if (length == 0) {                       // end of buffers
	        return buffers;
	      }
	      ByteBuffer buffer = ByteBuffer.allocate(length);
	      while (buffer.hasRemaining()) {
	        int p = buffer.position();
	        int i = in.read(buffer.array(), p, buffer.remaining());
	        if (i < 0)
	          throw new EOFException("Unexpected EOF");
	        buffer.position(p+i);
	      }
	      buffer.flip();
	      buffers.add(buffer);
	    }
	  }

	  static void writeBuffers(List<ByteBuffer> buffers, OutputStream out)
	    throws IOException {
	    for (ByteBuffer buffer : buffers) {
	      writeLength(buffer.limit(), out);           // length-prefix
	      out.write(buffer.array(), buffer.position(), buffer.remaining());
	      buffer.position(buffer.limit());
	    }
	    writeLength(0, out);                          // null-terminate
	  }

	  private static void writeLength(int length, OutputStream out)
	    throws IOException {
	    out.write(0xff & (length >>> 24));
	    out.write(0xff & (length >>> 16));
	    out.write(0xff & (length >>> 8));
	    out.write(0xff & length);
	  }
}
