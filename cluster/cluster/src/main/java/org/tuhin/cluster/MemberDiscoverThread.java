package org.tuhin.cluster;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;


public class MemberDiscoverThread extends Thread {

	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(MemberDiscoverThread.class);


	private boolean ready = false;
	private boolean error = false;
	private Exception exception = null;
	private boolean stop = false;
	
	private ClusterService service;
	


	public MemberDiscoverThread(String name, ClusterService service) {
		super(name);
		this.service = service;
	}
	public boolean isReady() {
		return ready;
	}
	public boolean isError() {
		return error;
	}
	public Exception getException() {
		return exception;
	}

	public void run() {
		if (logger.isDebugEnabled()) {
			logger.debug("run() - start");
		}

		try( MulticastSocket multicastSocket = new MulticastSocket(service.getConfig().getMulticastPort())){
			multicastSocket.joinGroup(InetAddress.getByName(service.getConfig().getMulticastGroup()));
			byte[] receiveData = new byte[1024];
			while(!stop){
				DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
				multicastSocket.receive(receivePacket);
				int senderMemberPort = ByteBuffer.wrap(receivePacket.getData()).asIntBuffer().get();
				InetAddress address = receivePacket.getAddress();
				ClusterMember clusterNode = new ClusterMember(address, senderMemberPort);
				service.addMemberToCluster(clusterNode);
				ready = true;
			}
		} catch (Exception ex) {
			logger.error("run()", ex);

			ready=true;
			error=true;
			exception=ex;
			throw new RuntimeException(ex);
		}

		if (logger.isDebugEnabled()) {
			logger.debug("run() - end");
		}
	}
	public void setStop(boolean b) {
		this.stop = b;
		if ( b ) {
			interrupt();
		}
		
	}

}
