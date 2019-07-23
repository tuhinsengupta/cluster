package org.tuhin.cluster.test;
import java.io.IOException;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.tuhin.cluster.ClusterConfig;
import org.tuhin.cluster.ClusterMember;
import org.tuhin.cluster.ClusterService;
import org.tuhin.cluster.ClusterServiceException;
import org.tuhin.cluster.RunStatus;


public class TestThrottle {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClusterServiceException 
	 * @throws ExecutionException 
	 * @throws NoSuchProviderException 
	 * @throws NoSuchAlgorithmException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClusterServiceException, ExecutionException, NoSuchAlgorithmException, NoSuchProviderException {

		//Appender app = new ConsoleAppender(new PatternLayout("%r [%t] %p %c %x - %m%n"));
		
		int testSize = 1000;
		//BasicConfigurator.configure(new NullAppender());
		BasicConfigurator.configure();
		
		Logger root = Logger.getRootLogger();
		root.setLevel(Level.INFO);

		ClusterService service  = ClusterService.getInstance(new ClusterConfig().setSocketBacklog(100));

		System.out.println("Started node : " + service.getCurrent().toString());

		if ( args.length == 1 && args[0].equals("start")){

			final ClusterMember member = new ClusterMember(service, InetAddress.getByName("LTUHS01"), 9301);

			while(true){
				ExecutorService pool = Executors.newFixedThreadPool(testSize);


				List<Future<RunStatus>> statuses = new ArrayList<Future<RunStatus>>();

				for ( int i=0; i < testSize; i++){
					Future<RunStatus> status = pool.submit(new Callable<RunStatus>() {

						public RunStatus call() throws Exception {
							try{
								return  member.isRunning(2000);
							}catch(IOException e){
								Throwable t = e;
								while ( t!= null){
									t = e.getCause();
								}
								return new RunStatus(RunStatus.Status.NotRunning,-1,-1, UUID.randomUUID());
							}
						}
					});
					statuses.add(status);
				}

				System.out.println(testSize + " status request sent to leader node.");

				pool.shutdown();

				pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);


				pool.shutdownNow();

				int l=0;
				int r=0;
				int n=0;
				for(Future<RunStatus> status:statuses){
					if ( status.get().isLeader()){
						l++;
					}
					if ( status.get().isMember()){
						r++;
					}
					if ( !status.get().isRunning()){
						n++;
					}
				}


				System.out.print("Leader      = " + l);
				System.out.print("  Member      = " + r);
				System.out.println("  Not Running = " + n);
				
				if ( l!= testSize){
					break;
				}
			}
		}

	}

}
