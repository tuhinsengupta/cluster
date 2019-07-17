package org.tuhin.cluster.multicast;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.tuhin.cluster.ClusterMember;
import org.tuhin.cluster.ClusterService;

/**
 * MulticastRegistry
 */
public class CastRegistry implements AutoCloseable {

	// logging output
	private static final Logger logger = Logger.getLogger(CastRegistry.class);

	private static final int DEFAULT_CAST_PORT = 1234;

	protected static final long CAST_SEND_INTERVAL = 1000;

	private final InetAddress castAddress;

	private final DatagramSocket castSocket;

	private final int castPort;
	
	private final ClusterService service;

	public CastRegistry(String host, int port, String id, TCPNode node, ClusterService service) {

		this.service = service;
		try {
			if ( host != null ) {
				castAddress = InetAddress.getByName(host);
			}else {
				castAddress = null;
			}
			castPort = port <= 0 ? DEFAULT_CAST_PORT : port;
			if ( castAddress != null) { //Multicast mode
				checkMulticastAddress(castAddress);
				castSocket = new MulticastSocket(castPort);
				((MulticastSocket)castSocket).setTimeToLive(128);
				NetUtils.joinMulticastGroup((MulticastSocket)castSocket, castAddress);
			}else {   //Broadcast Mode
				castSocket = new MulticastSocket(castPort);
				castSocket.setBroadcast(true);
			}
			startSenderReceiver(id,node);


		} catch (IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	private void startSenderReceiver(String id, TCPNode node) {
		//receiver
		Thread receiver_thread = new Thread(new Runnable() {
			@Override
			public void run() {
				byte[] buf = new byte[2048];
				DatagramPacket recv = new DatagramPacket(buf, buf.length);
				while (!castSocket.isClosed()) {
					try {
						castSocket.receive(recv);
						CastRegistry.this.receive(buf, (InetSocketAddress) recv.getSocketAddress());
						Arrays.fill(buf, (byte) 0);
					} catch (Throwable e) {
						if (!castSocket.isClosed()) {
							logger.error(e.getMessage(), e);
						}
					}
				}
			}
		}, "CastRegistryReceiver");
		receiver_thread.setDaemon(true);
		receiver_thread.start();

		//Sender
		Thread sender_thread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (!castSocket.isClosed()) {
					try {
						cast(id, node);
						Thread.sleep(CAST_SEND_INTERVAL);
					} catch (Throwable e) {
						if (!castSocket.isClosed()) {
							logger.error(e.getMessage(), e);
						}
					}
				}
			}
		}, "CastRegistrySender");
		sender_thread.setDaemon(true);
		sender_thread.start();


	}
	private void checkMulticastAddress(InetAddress multicastAddress) {
		if (!multicastAddress.isMulticastAddress()) {
			String message = "Invalid multicast address " + multicastAddress;
			if (!(multicastAddress instanceof Inet4Address)) {
				throw new IllegalArgumentException(message + ", " +
						"ipv4 multicast address scope: 224.0.0.0 - 239.255.255.255.");
			} else {
				throw new IllegalArgumentException(message + ", " + "ipv6 multicast address must start with ff, " +
						"for example: ff01::1");
			}
		}
	}

	private void receive(byte[] buf, InetSocketAddress remoteAddress) throws UnknownHostException {
		if (logger.isInfoEnabled()) {
			logger.info("Receive UDP message from " + remoteAddress);
		}

		byte[] idSize = Arrays.copyOfRange(buf, 0, 4);
		int idLength = ByteBuffer.wrap(idSize).asIntBuffer().get();

		byte[] idBytes = Arrays.copyOfRange(buf, 4, 4+idLength);
		String id = new String(idBytes);

		byte[] hostSize = Arrays.copyOfRange(buf, 4+idLength, 8+idLength);
		int hostStringLength = ByteBuffer.wrap(hostSize).asIntBuffer().get();

		byte[] hostBytes = Arrays.copyOfRange(buf, 8+idLength, 8+idLength+hostStringLength);
		String host = new String(hostBytes);

		byte[] portBytes = Arrays.copyOfRange(buf, 8+idLength+hostStringLength, buf.length);
		int port = ByteBuffer.wrap(portBytes).asIntBuffer().get();


		ClusterMember clusterNode = new ClusterMember(InetAddress.getByName(host), port);
		clusterNode.setId(UUID.fromString(id));
		service.addMemberToCluster(clusterNode);

	}

	private void cast(String id, TCPNode node) {
		try {
			byte[] idBytes = id.getBytes();
			byte[] hostBytes = node.getHost().getBytes();
			byte[] data = new byte[4+idBytes.length+4+hostBytes.length+4];

			//id

			System.arraycopy(ByteBuffer.allocate(4).putInt(idBytes.length).array(), 0, data, 0, 4);
			System.arraycopy(idBytes, 0, data, 4, idBytes.length);

			//Host
			System.arraycopy(ByteBuffer.allocate(4).putInt(hostBytes.length).array(), 0, data, 4+idBytes.length, 4);
			System.arraycopy(hostBytes, 0, data, 8+idBytes.length, hostBytes.length);

			//port
			System.arraycopy(ByteBuffer.allocate(4).putInt(node.getPort()).array(), 0, data, 8+idBytes.length+hostBytes.length, 4);

			if ( castAddress != null) {
				DatagramPacket hi = new DatagramPacket(data, data.length, castAddress, castPort);
				if (logger.isInfoEnabled()) {
					logger.info("Sending multicast UDP message to " + castAddress + ":" + castPort);
				}
				castSocket.send(hi);
			}else {
				for ( InetAddress broadcastAddress : NetUtils.listAllBroadcastAddresses()) {
					DatagramPacket hi = new DatagramPacket(data, data.length, broadcastAddress, castPort);
					if (logger.isInfoEnabled()) {
						logger.info("Sending broadcast UDP message to " + broadcastAddress + ":" + castPort);
					}
					castSocket.send(hi);					
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public void close() throws IOException {
		if ( castSocket != null) {
			castSocket.close();
		}	
	}

}