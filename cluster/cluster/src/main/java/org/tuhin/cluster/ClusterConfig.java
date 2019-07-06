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
	private static InetAddress localHost;
	static{
		try {
			localHost = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			localHost = null;
		}
	}

	
	private ClusterMember currentMember;
	private int heartBeatInterval = 1000; //Default 10 Sec
	private int networkTimeout = 2000; //Default 2 sec
	private int waitForLeaderInterval = 2000; //Default 2 Sec

	private int socketBacklog = 50;

	private long maxWait = 30;
	
	private int multicastPort = 8888;
	private String multicastGroup = "224.0.1.0";


	public ClusterConfig setSocketBacklog(int socketBacklog) {
		this.socketBacklog = socketBacklog;
		logger.info("[Cluster Config] <setSocketBacklog> " + toString());
		return this;
	}

	public ClusterMember getCurrentMember() {
		return currentMember;
	}

	public ClusterConfig() throws IOException {
		this(DEFAULT_WEIGHT);
	}

	public ClusterConfig(int weight) throws IOException {
		currentMember = ClusterMember.allocateLocal(localHost, weight);
		logger.info("[Cluster Config] <Init> " + toString());
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
		return "ClusterConfig [currentMember=" + currentMember + ", heartBeatInterval="
				+ heartBeatInterval + ", networkTimeout=" + networkTimeout
				+ ", waitForLeaderInterval=" + waitForLeaderInterval 
				+ ", socketBacklog=" + socketBacklog + ", maxWait=" + maxWait + "]";
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

	public void setMulticastPort(int multicastPort) {
		this.multicastPort = multicastPort;
	}

	public String getMulticastGroup() {
		return multicastGroup;
	}

	public void setMulticastGroup(String multicastGroup) {
		this.multicastGroup = multicastGroup;
	}




	
	
}
