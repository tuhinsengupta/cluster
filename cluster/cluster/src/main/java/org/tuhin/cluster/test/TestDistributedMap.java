package org.tuhin.cluster.test;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.tuhin.cluster.ClusterConfig;
import org.tuhin.cluster.ClusterService;
import org.tuhin.cluster.ClusterServiceException;
import org.tuhin.cluster.DistributedMap;


public class TestDistributedMap {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClusterServiceException 
	 * @throws NoSuchProviderException 
	 * @throws NoSuchAlgorithmException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClusterServiceException, NoSuchAlgorithmException, NoSuchProviderException {
		//BasicConfigurator.configure(new NullAppender());
		BasicConfigurator.configure();
		
		//Logger.getRootLogger().setLevel(Level.INFO);

		ClusterService service  = ClusterService.getInstance(new ClusterConfig());


		DistributedMap<String,String> map = new DistributedMap<String,String>(service, "map");

		System.out.println(map.get("key1"));

		map.put("key1", "value1");


		System.out.println(map.get("key1"));

		System.out.println(map.keySet());

		System.out.println(map.values());

		DistributedMap<String,TestObject> map1 = new DistributedMap<String,TestObject>(service, "map1");

		map1.put("XXX", new TestObject("TestObject1"));

		System.out.println(map1.entrySet());

		Map<String, TestObject> newObj = new HashMap<String, TestObject>();
		newObj.put("key", new TestObject("new"));
		map1.putAll(newObj);

		//service.restart();

		System.out.println(map1.entrySet());

		//File zipFile = service.zipLogFiles("C:\\BANCSFS\\BancsProduct\\Intranet\\logs", "monitor.log");
		
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		String s;
		while ((s = in.readLine()) != null && s.length() != 0){
			String[] values = s.split(",");
			if ( values.length == 2){
				map.put(values[0], values[1]);
				System.out.println("Added " + values[0] + "=" + values[1]);
			}
			if ( values.length == 1){
				System.out.println(values[0] + "=" + map.get(values[0]));
			}
		}		
		//		while(true){
		//			Thread.sleep(1000);
		//			if ( service.isLeader() ){
		//				System.out.println(service.getMembers() );
		//			}
		//		}
		
		

	}

}
