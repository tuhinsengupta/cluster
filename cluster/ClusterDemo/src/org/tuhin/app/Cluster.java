package org.tuhin.app;

import java.io.IOException;
import java.util.Set;

import org.tuhin.cluster.ClusterConfig;
import org.tuhin.cluster.ClusterMember;
import org.tuhin.cluster.ClusterService;
import org.tuhin.cluster.ClusterServiceException;

public class Cluster {
	
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
		return service.getConfig().getMulticastGroup();
	}

}
