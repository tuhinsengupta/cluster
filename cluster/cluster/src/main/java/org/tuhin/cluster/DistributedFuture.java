package org.tuhin.cluster;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class DistributedFuture<V> implements Future<V>,Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 9184680232739337674L;
	
	private ClusterMember memeber;
	private String instanceId;


	public DistributedFuture(ClusterMember memeber, String instanceId) {
		super();
		this.memeber = memeber;
		this.instanceId = instanceId;
	}

	
	public boolean cancel(boolean interruptIfRunning) {
		try {
			ResultObject result = memeber.sendMessage(new ClusterMessage(ClusterMessage.Operation.Cancel, instanceId, interruptIfRunning));
			if ( result.isSuccess() ){
				return (Boolean)result.getResult();
			}else{
				return false;
			}
		} catch (IOException e) {
			return false;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

	
	public V get() throws InterruptedException, ExecutionException {
		
		try {
			return get(-1, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			throw new InterruptedException(e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	
	public V get(long paramLong, TimeUnit paramTimeUnit)
			throws InterruptedException, ExecutionException, TimeoutException {

		try {
			ResultObject result = memeber.sendMessage(new ClusterMessage(ClusterMessage.Operation.Get, instanceId, paramLong, paramTimeUnit));
			if ( result.isSuccess() ){
				return (V)result.getResult();
			}else{
				throw new ExecutionException(result.getException());
			}
		} catch (IOException e) {
			throw new ExecutionException(e);
		} catch (ClassNotFoundException e) {
			throw new ExecutionException(e);
		}
	}

	
	public boolean isCancelled() {
		try {
			ResultObject result = memeber.sendMessage(new ClusterMessage(ClusterMessage.Operation.IsCancelled, instanceId));
			if ( result.isSuccess() ){
				return (Boolean)result.getResult();
			}else{
				return false;
			}
		} catch (IOException e) {
			return false;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

	
	public boolean isDone() {
		try {
			ResultObject result = memeber.sendMessage(new ClusterMessage(ClusterMessage.Operation.IsDone, instanceId));
			if ( result.isSuccess() ){
				return (Boolean)result.getResult();
			}else{
				return false;
			}
		} catch (IOException e) {
			return false;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

}
