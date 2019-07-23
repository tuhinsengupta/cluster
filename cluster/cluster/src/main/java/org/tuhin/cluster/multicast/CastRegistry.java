package org.tuhin.cluster.multicast;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.util.Arrays;
import java.util.Set;

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

	public CastRegistry(String host, int port, ClusterService service) {

		this.service = service;
		try {
			if ( host != null ) {
				castAddress = Inet4Address.getByName(host);
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
			startSenderReceiver();


		} catch (IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	private void startSenderReceiver() {
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
						//Ignore the error
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
						cast();
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
			if (multicastAddress instanceof Inet4Address) {
				throw new IllegalArgumentException(message + ", " +
						"ipv4 multicast address scope: 224.0.0.0 - 239.255.255.255.");
			} else {
				throw new IllegalArgumentException(message + ", " + "ipv6 multicast address must start with ff, " +
						"for example: ff01::1");
			}
		}
	}

	private void receive(byte[] buf, InetSocketAddress remoteAddress) throws ClassNotFoundException, IOException {
		if (logger.isInfoEnabled()) {
			logger.info("Receive UDP message from " + remoteAddress);
		}

		Set<ClusterMember> members = unbundleData(buf);
		for(ClusterMember clusterNode:members) {
			clusterNode.setService(service);
			if(service.addMemberToCluster(clusterNode)) {
				clusterNode.setMembers(service.getConfig().getNetworkTimeout(), service.getMembers());
			}
		}
	}

	private byte[] bundleData() throws IOException {
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
		  out = new ObjectOutputStream(bos);   
		  out.writeObject(service.getMembers());
		  out.flush();
		  return bos.toByteArray();
		} finally {
		  try {
		    bos.close();
		  } catch (IOException ex) {
		  }
		}
	}
	@SuppressWarnings("unchecked")
	private Set<ClusterMember> unbundleData(byte[] data) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(data);
		ObjectInput in = null;
		try {
		  in = new ObjectInputStream(bis);
		  return (Set<ClusterMember>) in.readObject(); 
		} finally {
		  try {
		    if (in != null) {
		      in.close();
		    }
		  } catch (IOException ex) {
		  }
		}
	}
	private void cast() {
		try {
			byte[] data = bundleData();

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