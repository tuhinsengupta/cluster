package org.tuhin.app;

import java.io.IOException;
import java.util.Set;

import org.tuhin.cluster.ClusterConfig;
import org.tuhin.cluster.ClusterMember;
import org.tuhin.cluster.ClusterService;
import org.tuhin.cluster.ClusterServiceException;
import org.tuhin.cluster.multicast.NetUtils;

public class Cluster implements AutoCloseable{

	private static Cluster instance;

	private ClusterService service;
	private Cluster(String group, String peer, String port, String weight) throws ClusterServiceException, IOException {
		int node_port = 0;
		int node_weight = 1;
		try {
			node_port = Integer.parseInt(port);
		}catch(Exception e) {

		}
		try {
			node_weight = Integer.parseInt(weight);
		}catch(Exception e) {

		}

		if (peer != null) {
			service = ClusterService.getInstance(new ClusterConfig()
												.setPort(node_port)
												.setWeight(node_weight)
												.setMulticastGroup(group)
												.setPeerNode(peer)
									);
		}else {
			service = ClusterService.getInstance(new ClusterConfig()
												.setPort(node_port)
												.setWeight(node_weight)
												.setMulticastGroup(group)
									);
			
		}
	}


	public static Cluster getInstance(String group, String peer, String port, String weight) throws ClusterServiceException, IOException {
		if (instance == null) {
			instance = new Cluster(group, peer, port, weight);
		}

		return instance;
	}


	public Set<ClusterMember> getNodes() {

		return service.getMembers();
	}


	public void stop() throws IOException {
		if (service != null ) {
			service.stop();
		}

	}


	public String getName() {
		if ( service.getConfig().getMulticastGroup() != null ) {
			return "<Multicast :" + service.getConfig().getMulticastGroup() + ">";
		}else {
			return "<Broadcast :" + NetUtils.listAllBroadcastAddresses().toString() + ">";
		}
	}


	@Override
	public void close() throws Exception {
		stop();
	}


	public ClusterService getService() {
		return service;
	}

}
