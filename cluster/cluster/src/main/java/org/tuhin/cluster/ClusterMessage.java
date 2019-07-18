package org.tuhin.cluster;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class ClusterMessage implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1487257777667499439L;
	public static enum Operation { CreateMap, PutIntoMap, ClearMap, Sync, Stat, RemoveFromMap, PutAllIntoMap, Execute, AwaitTermination, IsShutdown, IsTerminated, ShutdownNow, Shutdown, CreateExecutorService, InvokeAll, InvokeAny, Cancel, Get, IsCancelled, IsDone, Submit,  Custom, UnsetLeader, ZipLogFiles, MemberList, SendMemberList }
	
	private Operation operation;
	private List<Object> args;

	public ClusterMessage(Operation operation, Object... arg) throws IOException {
		super();
		this.operation = operation;
		args = new ArrayList<Object>();
		
		for (Object s:arg){
			new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(s);
			args.add(s);
		}
		
	}
	public Operation getOperation() {
		return operation;
	}
	public List<?> getArgs() {
		return args;
	}
	@Override
	public String toString() {
		return "ClusterMessage [operation=" + operation + ", args=" + args
				+ "]";
	}
	
	
	
}
