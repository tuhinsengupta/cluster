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
	private Cluster() throws ClusterServiceException, IOException {
		
		 service = ClusterService.getInstance(new ClusterConfig());
	}

	
	public static Cluster getInstance() throws ClusterServiceException, IOException {
		if (instance == null) {
			instance = new Cluster();
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
