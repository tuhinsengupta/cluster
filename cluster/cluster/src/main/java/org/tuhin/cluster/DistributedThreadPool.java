package org.tuhin.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import org.tuhin.cluster.utils.CollectionUtils;


public class DistributedThreadPool implements ExecutorService{

	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ClusterService.class);

	private static final int DEFAULT_THREAD_POOL_SIZE = 5;

	private ClusterService service;
	private TaskDistributingPolicy  distributingPolicy;

	private static Random randomGenerator = new Random();

	private static int roundRobinIndex = 0;
	private static int weightedRoundRobinIndex = 0;

	private String instanceId;

	public DistributedThreadPool(ClusterService service, int threadPoolSize) throws ClusterServiceException, IOException, InterruptedException {
		this(service, TaskDistributingPolicy.RoundRobin, threadPoolSize);
	}

	public DistributedThreadPool(ClusterService service) throws ClusterServiceException, IOException, InterruptedException {
		this(service, TaskDistributingPolicy.RoundRobin, DEFAULT_THREAD_POOL_SIZE);
	}

	public DistributedThreadPool(ClusterService service, TaskDistributingPolicy policy) throws ClusterServiceException, IOException, InterruptedException {
		this(service, policy, DEFAULT_THREAD_POOL_SIZE);
	}
	public DistributedThreadPool(ClusterService service, TaskDistributingPolicy policy, int threadPoolSize) throws ClusterServiceException, IOException, InterruptedException {
		super();

		this.service = service;

		this.instanceId = UUID.randomUUID().toString();

		this.distributingPolicy = policy;

		service.createExecutorService(instanceId, threadPoolSize);

	}


	private ClusterMember choose(Set<ClusterMember> members) {

		if ( distributingPolicy == TaskDistributingPolicy.Random){
			return chooseRandom(members);
		}

		if ( distributingPolicy == TaskDistributingPolicy.RoundRobin){
			return chooseRoundRobin(members);
		}
		if ( distributingPolicy == TaskDistributingPolicy.WeightedRoundRobin){
			return chooseWeightedRoundRobin(members);
		}
		return chooseNth(members, 0);
	}

	private ClusterMember chooseNth(Collection<ClusterMember> members, int i) {
		int index = 0;
		for ( ClusterMember member:members){
			if ( index == i){
				return member;
			}
			index++;
		}

		return null;
	}


	private ClusterMember chooseRoundRobin(Set<ClusterMember> members) {
		int size = members.size();

		if ( size > 0 ){
			roundRobinIndex++;
			if ( roundRobinIndex == size) {
				roundRobinIndex = 0;
			}
			return chooseNth(members,roundRobinIndex);

		}else{
			return null;
		}
	}

	private ClusterMember chooseWeightedRoundRobin(Set<ClusterMember> members) {
		List<ClusterMember> inflatedMembers = new ArrayList<ClusterMember>();
		
		for(ClusterMember member:members){
			for(int i=0 ;i < member.getWeight(); i++){
				inflatedMembers.add(member);
			}
		}
		int size = inflatedMembers.size();

		if ( size > 0 ){
			weightedRoundRobinIndex++;
			if ( weightedRoundRobinIndex == size) {
				weightedRoundRobinIndex = 0;
			}
			return chooseNth(inflatedMembers,weightedRoundRobinIndex);

		}else{
			return null;
		}
	}


	private ClusterMember chooseRandom(Set<ClusterMember> members) {
		int size = members.size();

		if ( size > 0 ){

			return chooseNth(members,randInt(0,size-1));

		}else{
			return null;
		}
	}


	private int randInt(int min, int max) {


		int randomNum = randomGenerator.nextInt((max - min) + 1) + min;

		return randomNum;
	}


	
	public void execute(Runnable arg0) {
		ClusterMember member = choose(service.getMembers());
		
		if ( member == null ){
			logger.error("None of the cluster node running (Shutting down?), executing in current thread!");
			arg0.run();
			return;
		}

		try {
			service.execute(member, instanceId, arg0);
		} catch (IOException e) {
			logger.error("Could not execute on designated member");

			if ( !member.isCurrent()){
				logger.info("Executing on current member");

				try {
					service.execute(service.findCurrent(),instanceId,arg0);
				} catch (IOException e1) {
					throw new RuntimeException(e1);
				}
			}else{
				throw new RuntimeException(e);
			}
		}

	}


	
	public boolean awaitTermination(long arg0, TimeUnit arg1) throws InterruptedException{

		try {
			return service.awaitTermination(instanceId, arg0,arg1);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> arg0)
			throws InterruptedException {

		return invokeAll(arg0,-1, TimeUnit.SECONDS);
	}

	
	public <T> List<Future<T>> invokeAll(
			final Collection<? extends Callable<T>> arg0,final long arg1,final TimeUnit arg2)
					throws InterruptedException {

		int noMembers = service.getMembers().size();

		List<Collection<? extends Callable<T>>> dividedList = CollectionUtils.split(arg0, noMembers);

		ExecutorService pool = Executors.newFixedThreadPool(noMembers);

		List<Callable<List<Future<T>>>> newInvokeList = new ArrayList<Callable<List<Future<T>>>>();
		for(final Collection<? extends Callable<T>> list:dividedList){
			newInvokeList.add(new Callable<List<Future<T>>>() {

				
				public List<Future<T>> call() throws Exception {
					return _invokeAll(list,arg1, arg2);
				}
			});
		}

		List<Future<List<Future<T>>>> returns;
		if ( arg1 == -1){
			returns = pool.invokeAll(newInvokeList);
		}else{
			returns = pool.invokeAll(newInvokeList,arg1, arg2);
		}

		pool.shutdown();

		pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

		List<Future<T>> finalReturn = new ArrayList<Future<T>>();

		try{
			for(Future<List<Future<T>>> r:returns){
				List<Future<T>> ret;
				if ( arg1 == -1){
					ret = r.get();
				}else{
					ret = r.get(arg1,arg2);
				}
				finalReturn.addAll(ret);
			}
		}catch(ExecutionException e){
			throw new RuntimeException(e);
		}catch(TimeoutException e){
			throw new RuntimeException(e);
		}

		return finalReturn;

	}



	private <T> List<Future<T>> _invokeAll(
			Collection<? extends Callable<T>> arg0, long arg1, TimeUnit arg2)
					throws InterruptedException {

		ClusterMember member = choose(service.getMembers());


		try {

			return service.invokeAll(member, instanceId, arg0, arg1, arg2);
		} catch (IOException e) {
			logger.error("Could not execute on designated member");
			if ( !member.isCurrent()){
				logger.info("Executing on current member");

				try {
					return service.invokeAll(service.findCurrent(), instanceId, arg0, arg1, arg2);
				} catch (IOException e1) {
					throw new RuntimeException(e1);
				}
			}else{
				throw new RuntimeException(e);
			}
		}
	}

	
	public <T> T invokeAny(Collection<? extends Callable<T>> arg0)
			throws InterruptedException, ExecutionException {
		try {
			return invokeAny(arg0,-1, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			throw new InterruptedException(e.getMessage());
		}
	}
	
	
	public <T> T invokeAny(
			final Collection<? extends Callable<T>> arg0,final long arg1,final TimeUnit arg2)
					throws InterruptedException, TimeoutException {

		int noMembers = service.getMembers().size();

		List<Collection<? extends Callable<T>>> dividedList = CollectionUtils.split(arg0, noMembers);

		ExecutorService pool = Executors.newFixedThreadPool(noMembers);

		List<Callable<T>> newInvokeList = new ArrayList<Callable<T>>();
		for(final Collection<? extends Callable<T>> list:dividedList){
			newInvokeList.add(new Callable<T>() {

				
				public T call() throws Exception {
					return _invokeAny(list,arg1, arg2);
				}
			});
		}

		T ret;

		try{
			if ( arg1 == -1){
				ret = pool.invokeAny(newInvokeList);
			}else{
				ret = pool.invokeAny(newInvokeList,arg1, arg2);
			}
		}catch(ExecutionException e){
			throw new RuntimeException(e);
		}

		pool.shutdown();

		pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);


		return ret;

	}


	private <T> T _invokeAny(Collection<? extends Callable<T>> arg0, long arg1,
			TimeUnit arg2) throws InterruptedException, ExecutionException,
			TimeoutException {
		ClusterMember member = choose(service.getMembers());
		try {
			return service.invokeAny(member, instanceId, arg0, arg1, arg2);

		} catch (IOException e) {
			logger.error("Could not execute on designated member");
			if ( !member.isCurrent()){
				logger.info("Executing on current member");

				try {
					return service.invokeAny(service.findCurrent(), instanceId, arg0, arg1, arg2);
				} catch (IOException e1) {
					throw new RuntimeException(e1);
				}
			}else{
				throw new RuntimeException(e);
			}
		}
	}

	
	public boolean isShutdown() {

		try {
			return service.isShutdown(instanceId);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			return false;
		}
	}

	
	public boolean isTerminated() {

		try {
			return service.isTerminated(instanceId);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			return false;
		}
	}

	
	public void shutdown() {
		try {
			service.shutdown(instanceId);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
		}
	}

	
	public List<Runnable> shutdownNow() {
		try {
			return service.shutdownNow(instanceId);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			return new ArrayList<Runnable>();
		}
	}

	
	public <T> Future<T> submit(Callable<T> arg0) {


		ClusterMember member = choose(service.getMembers());

		try {
			return service.submit(member, instanceId, arg0);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	
	public Future<?> submit(Runnable arg0) {
		return submit(new RunnableAdapter<Object>(arg0,null));
	}

	
	public <T> Future<T> submit(Runnable arg0, T arg1) {
		return submit(new RunnableAdapter<T>(arg0,arg1));
	}

	static final class RunnableAdapter<T>
	implements Callable<T>,Serializable
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 790626830722481632L;
		final Runnable task;
		final T result;

		RunnableAdapter(Runnable paramRunnable, T paramT)
		{
			this.task = paramRunnable;
			this.result = paramT;
		}

		public T call()
		{
			this.task.run();
			return this.result;
		}
	}

}
