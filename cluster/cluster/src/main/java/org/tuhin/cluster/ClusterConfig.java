package org.tuhin.cluster;


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;


public class ClusterConfig {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ClusterConfig.class);
	
	private static int DEFAULT_WEIGHT = 1;
	private static int DEFAULT_PORT   = 0;
	private static InetAddress localHost;
	static{
		try {
			localHost = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			localHost = null;
		}
	}


	private int weight = DEFAULT_WEIGHT;
	private int port   = DEFAULT_PORT;
	private ClusterMember currentMember;
	private int heartBeatInterval = 1000; //Default 1 Sec
	private int networkTimeout = 2000; //Default 2 sec
	private int waitForLeaderInterval = 2000; //Default 2 Sec
	private int socketBacklog = 50;
	private long maxWait = 30;
	private int multicastPort = 8888;
	private String multicastGroup = null; //default is broadcast
	private String peerNode = null;

	public String getPeerNode() {
		return peerNode;
	}

	public ClusterConfig setPeerNode(String peerNode) {
		this.peerNode = peerNode;
		return this;
	}

	public ClusterConfig setSocketBacklog(int socketBacklog) {
		this.socketBacklog = socketBacklog;
		logger.info("[Cluster Config] <setSocketBacklog> " + toString());
		return this;
	}

	public ClusterMember getCurrentMember(ClusterService service) throws IOException {
		if ( currentMember == null) {
			try {
				currentMember = ClusterMember.allocateLocal(service, localHost, weight, port);
			} catch (ClusterServiceException e) {
				throw new IOException(e);
			}
		}
		return currentMember;
	}

	public ClusterConfig(){
	}

	public int getHeartBeatInterval() {
		return heartBeatInterval;
	}

	public ClusterConfig setHeartBeatInterval(int heartBeatInterval) {
		this.heartBeatInterval = heartBeatInterval;
		this.waitForLeaderInterval = heartBeatInterval*2; // Wait for leader should 2 times more than heartbeat
		logger.info("[Cluster Config] <setHeartBeatInterval> " + toString());
		return this;
	}

	public ClusterConfig setMaxWait(int minutes) {
		this.maxWait = minutes;
		logger.info("[Cluster Config] <setMaxWait> " + toString());
		return this;
	}

	
	public int getNetworkTimeout() {
		return networkTimeout;
	}

	public ClusterConfig setNetworkTimeout(int networkTimeout) {
		this.networkTimeout = networkTimeout;
		logger.info("[Cluster Config] <setNetworkTimeout> " + toString());
		return this;
	}

	public int getWaitForLeaderInterval() {
		return waitForLeaderInterval;
	}



	@Override
	public String toString() {
		return "ClusterConfig [weight=" + weight + ", port=" + port + ", currentMember=" + currentMember
				+ ", heartBeatInterval=" + heartBeatInterval + ", networkTimeout=" + networkTimeout
				+ ", waitForLeaderInterval=" + waitForLeaderInterval + ", socketBacklog=" + socketBacklog + ", maxWait="
				+ maxWait + ", multicastPort=" + multicastPort + ", multicastGroup=" + multicastGroup + ", peerNode="
				+ peerNode + "]";
	}

	public int getSocketBacklog() {
		return socketBacklog;
	}

	public long maxWait() {
		return maxWait;
	}

	public int getMulticastPort() {
		return multicastPort;
	}

	public ClusterConfig setMulticastPort(int multicastPort) {
		this.multicastPort = multicastPort;
		return this;
	}

	public String getMulticastGroup() {
		return multicastGroup;
	}

	public ClusterConfig setMulticastGroup(String multicastGroup) {
		this.multicastGroup = multicastGroup;
		return this;
	}

	public int getWeight() {
		return weight;
	}

	public ClusterConfig setWeight(int weight) {
		this.weight = weight;
		return this;
	}

	public int getPort() {
		return port;
	}

	public ClusterConfig setPort(int port) {
		this.port = port;
		return this;
	}




	
	
}
