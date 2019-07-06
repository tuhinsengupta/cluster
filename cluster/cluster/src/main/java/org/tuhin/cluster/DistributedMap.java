package org.tuhin.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class DistributedMap<K,V> implements Map<K, V>{

	private ClusterService service;
	private String uniqueName;

	public DistributedMap(ClusterService service, String name) throws ClusterServiceException, IOException, InterruptedException {
		super();

		this.service = service;

		this.uniqueName = name;

		service.createMap(uniqueName);
	}

	public void clear() {
		try {
			service.clear(uniqueName);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean containsKey(Object arg0) {
		return service.containsKey(uniqueName, arg0);
	}

	public boolean containsValue(Object arg0) {
		return service.containsValue(uniqueName, arg0);
	}

	public Set<java.util.Map.Entry<K, V>> entrySet() {
		Set<java.util.Map.Entry<K, V>> entrySet = new HashSet<java.util.Map.Entry<K, V>>();
		Set<java.util.Map.Entry<Object, Object>> storedVals = service.entrySet(uniqueName);
		for(java.util.Map.Entry<Object, Object> entry:storedVals){
			@SuppressWarnings("unchecked")
			java.util.Map.Entry<K, V> e = (java.util.Map.Entry<K, V>)entry;
			entrySet.add(e);
		}
		return entrySet;
	}

	@SuppressWarnings("unchecked")
	public V get(Object arg0) {
		return (V)service.get(uniqueName, arg0);
	}

	public boolean isEmpty() {
		return service.isEmpty(uniqueName);
	}

	public Set<K> keySet() {
		
		Set<K> keySet = new HashSet<K>();
		
		for(Object k:service.keySet(uniqueName)){
			@SuppressWarnings("unchecked")
			K k2 = (K)k;
			keySet.add(k2);
		}
		return keySet;
	}

	@SuppressWarnings("unchecked")
	public V put(K arg0, V arg1) {
		try {
			return (V)service.put(uniqueName, arg0, arg1);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void putAll(Map<? extends K, ? extends V> arg0) {
		synchronized (arg0) {

			Map<Object,Object> map = new HashMap<Object,Object>();
			for(Map.Entry<? extends K, ? extends V> entry:arg0.entrySet()){
				Object key = entry.getKey();
				Object value = entry.getValue();
				map.put(key, value);
			}
			try {
				service.putAll(uniqueName, map);
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

	}

	@SuppressWarnings("unchecked")
	public V remove(Object arg0) {
		try {
			return (V)service.remove(uniqueName, arg0);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public int size() {
		return service.size(uniqueName);
	}

	public Collection<V> values() {
		Collection<V> values = new ArrayList<V>();
		for(Object obj : service.values(uniqueName)){
			@SuppressWarnings("unchecked")
			V v = (V)obj;
			values.add(v);
		}
		return values;
	}

}
