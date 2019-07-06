package org.tuhin.cluster;

import java.io.Serializable;
import java.util.UUID;


public class RunStatus implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2537968247319314645L;


	public static enum Status{Leader, Member, NotRunning};
	
	private Status status;
	private long started = -1;
	private long startedAsLeader = -1;
	private String id;
	

	public RunStatus(Status status, long started, long startedAsLeader, UUID id) {
		super();
		this.status = status;
		this.started = started;
		this.startedAsLeader = startedAsLeader;
		this.id = id.toString();
	}


	@Override
	public String toString() {
		switch (status) {
		case Leader:
			return "Running as Leader";
		case Member:
			return "Running";
		case NotRunning:
			return "Not Running";

		default:
			return super.toString();
		}
	}


	public UUID getId() {
		return UUID.fromString(id);
	}

	public Status getStatus() {
		return status;
	}


	public void setStatus(Status status) {
		this.status = status;
	}


	public long getStarted() {
		return started;
	}


	public void setStarted(long started) {
		this.started = started;
	}


	public long getStartedAsLeader() {
		return startedAsLeader;
	}


	public void setStartedAsLeader(long startedAsLeader) {
		this.startedAsLeader = startedAsLeader;
	}


	public boolean isRunning() {
		return status != Status.NotRunning;
	}


	public boolean isLeader() {
		return status == Status.Leader;
	}


	public boolean isMember() {
		return status == Status.Member;
	}
	
	

}
