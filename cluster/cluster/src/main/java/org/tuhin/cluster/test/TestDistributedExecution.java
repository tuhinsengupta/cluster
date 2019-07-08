package org.tuhin.cluster.test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;
import org.tuhin.cluster.ClusterConfig;
import org.tuhin.cluster.ClusterService;
import org.tuhin.cluster.ClusterServiceException;
import org.tuhin.cluster.DistributedThreadPool;
import org.tuhin.cluster.TaskDistributingPolicy;


public class TestDistributedExecution {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClusterServiceException 
	 * @throws ExecutionException 
	 */
	public static void main(String[] args) throws ClusterServiceException, IOException, InterruptedException  {
		//BasicConfigurator.configure(new NullAppender());
		BasicConfigurator.configure();


		ClusterService service  = ClusterService.getInstance(new ClusterConfig());


		DistributedThreadPool pool = new DistributedThreadPool(service, TaskDistributingPolicy.RoundRobin, 20);

		
			final Random rnd = new Random();
			
/*
			for(int i = 0; i < 20; i++){
				final int j = i;
				pool.execute(new SerilizableRunnable()  {

					private static final long serialVersionUID = 4216316174587936132L;

					@Override
					public void run() {
						int x = rnd.nextInt(11000) + 1000;
						System.err.println("[" + j + "] Sleeping for " + x + " ms");
						try {
							Thread.sleep(x);
						} catch (InterruptedException e) {
						}
					}
				});
				//Thread.sleep(1000);

			}
*/
			
//			for(int i = 0; i < 20; i++){
//				final int j = i;
//				Future<?> f = pool.submit(new SerilizableRunnable()  {
//
//					private static final long serialVersionUID = 4216316174587936132L;
//
//					@Override
//					public void run() {
//						int x = rnd.nextInt(11000) + 1000;
//						System.err.println("[" + j + "] Sleeping for " + x + " ms");
//						try {
//							Thread.sleep(x);
//						} catch (InterruptedException e) {
//						}
//					}
//				});
//				
//				//Thread.sleep(1000);
//
//			}

			
			List<Future<String>> futures = new ArrayList<Future<String>>();
			List<SerilizableCallable<String>> calls = new ArrayList<SerilizableCallable<String>>();
			
			for(int i = 0; i < 20; i++){
				final int j = i;
				calls.add(new SerilizableCallable<String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -2561764440502398454L;

					public String call() throws Exception {
						int x = rnd.nextInt(20);
						System.err.println("[" + j + "] " + x);
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
						}
						return "V=" + x; 
					}
				});
				
				//Thread.sleep(1000);

			}
			
			pool.submit(new SerilizableRunnable() {
				
				public void run() {
					System.out.println("Hello World!");
					
				}
			});
			try {
				futures = pool.invokeAll(calls);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}

//			String res = null;
//			try {
//				res = pool.invokeAny(calls);
//			} catch (InterruptedException e1) {
//				e1.printStackTrace();
//			} catch (ExecutionException e) {
//				e.printStackTrace();
//			}

			pool.shutdown();
			try {
				pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
			}
			System.out.println("All Execution done");
			for(Future<String> f:futures){
				try {
					System.out.println(f.get());
					//System.out.println(f.get());
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
			
			//System.out.println(res);
			
			

	}

}
