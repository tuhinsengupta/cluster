package org.tuhin.cluster;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;


public class SyncData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8740568923608052044L;
	
	private Map<String,Map<Object,Object>> mapStore ;

	private Map<String, Integer> servicePools ;
	

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public Map<String, Map<Object, Object>> getMapStore() {
		return mapStore;
	}

	public Map<String,Integer> getServicePools() {
		return servicePools;
	}

	public SyncData(Map<String, Map<Object, Object>> mapStore,
			Map<String, ThreadPoolExecutor> servicePool) {
		super();
		this.mapStore = mapStore;
		this.servicePools = new HashMap<String, Integer>();
		for(String name: servicePool.keySet()){
			this.servicePools.put(name, servicePool.get(name).getCorePoolSize());
		}
	}
	
	


}
