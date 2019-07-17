package org.tuhin.cluster.multicast;

public class TCPNode {

	private final String host;
	private final int port;
	public TCPNode(String host, int port) {
		super();
		this.host = host;
		this.port = port;
	}
	public String getHost() {
		return host;
	}
	public int getPort() {
		return port;
	}
	@Override
	public String toString() {
		return "TCPNode [host=" + host + ", port=" + port + "]";
	}
	
	
}
