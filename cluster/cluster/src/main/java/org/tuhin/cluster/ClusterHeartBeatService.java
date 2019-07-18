package org.tuhin.cluster;

import java.io.IOException;
import java.util.Set;

import org.apache.log4j.Logger;


public class ClusterHeartBeatService implements Runnable {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger
			.getLogger(ClusterHeartBeatService.class);

	private static final String CLUSTER_SERVICE_LEADER_ASSIGN_THREAD_NAME = "ClusterService-AsigningLeader";

	/**
	 * 
	 */
	final ClusterService clusterService;

	/**
	 * @param clusterService
	 */
	ClusterHeartBeatService(ClusterService clusterService) {
		this.clusterService = clusterService;
	}

	boolean assigningNewLeader=false;


	public void run() {
		if (logger.isDebugEnabled()) {
			logger.debug("Starting heartbeat service.");
		}

		while(!clusterService.isStopped()){
			try {
				Thread.sleep(clusterService.config.getHeartBeatInterval());
			} catch (InterruptedException e) {
				if ( !clusterService.stopRequested ){
					logger.error("run()", e);
				}
			}
			if ( clusterService.stopRequested ){
				logger.info("Stop requested, exiting...");
				break;
			}
			int leaders = 0;
			int myPosition = 0;
			int index = 0;
			Set<ClusterMember> members = clusterService.getMembers();
			for ( ClusterMember member : members ){
				try{
					RunStatus stat = member.isRunning(clusterService.config.getNetworkTimeout());
					if ( stat.isRunning() ){
						if (logger.isDebugEnabled()) {
							logger.debug("[Heartbeat Service] Member running :" + member + " [" + stat + "]" );
						}

						//clusterService.joinedMemebers.add(member);
						index++;
						if ( member.isCurrent() ){
							myPosition = index;
						}
						if ( stat.isLeader() ){
							leaders++;
							clusterService.setLeadMember(member);
						}
					}else{
						if (logger.isDebugEnabled()) {
							logger.debug("[Heartbeat Service] Member not running :" + member );
						}
						clusterService.removeMemberFromCluster(member);
					}
				}catch(IOException e){
					logger.warn("Could not get status of node " + member.toString() + ", assuming dead. [" +  e.getMessage() + "]");
					clusterService.removeMemberFromCluster(member);
				}
			}
			if ( leaders==0 ){
				if (logger.isDebugEnabled()) {
					logger.debug("[Heartbeat Service] No leader found." );
				}

				assignLeaderThread(myPosition);
			}
			if ( leaders>1){
				if (logger.isDebugEnabled()) {
					logger.debug("[Heartbeat Service] multiple leaders found." );
				}
				try {
					clusterService.restart();
					break;
				} catch (ClusterServiceException e) {
					logger.error("run()", e);
				} catch (IOException e) {
					logger.error("run()", e);
				}
			}
		}

	}

	private void assignLeaderThread(final int myPosition) {
		if (logger.isDebugEnabled()) {
			logger.debug("assignLeaderThread(int) - start");
		}

		if ( assigningNewLeader) {
			if (logger.isDebugEnabled()) {
				logger.debug("assignLeaderThread(int) - end");
			}
			return;
		}
		(new Thread(new ClusterAssignLeaderService(this, myPosition),CLUSTER_SERVICE_LEADER_ASSIGN_THREAD_NAME)).start();

		if (logger.isDebugEnabled()) {
			logger.debug("assignLeaderThread(int) - end");
		}
	}
}