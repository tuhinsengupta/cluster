package org.tuhin.cluster;

import java.io.IOException;

import org.apache.log4j.Logger;

final class ClusterAssignLeaderService implements Runnable {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger
		.getLogger(ClusterAssignLeaderService.class);

	/**
	 * 
	 */
	private final ClusterHeartBeatService clusterHeartBeatService;
	private final int myPosition;

	ClusterAssignLeaderService(ClusterHeartBeatService clusterHeartBeatService, int myPosition) {
		this.clusterHeartBeatService = clusterHeartBeatService;
		this.myPosition = myPosition;
	}

	public void run() {
		if (logger.isDebugEnabled()) {
			logger.debug("Leader assigning thread starting.");
		}

		this.clusterHeartBeatService.assigningNewLeader = true;
		boolean leaderPresent = false;

		logger.info("[Leader asigning thread] No leader present - finding leader in " + myPosition + " loops");
		for(int i=0; i < myPosition; i++){
			logger.info("[Leader asigning thread] No leader present - sleeping for " + this.clusterHeartBeatService.clusterService.config.getWaitForLeaderInterval() + " ms");
			try {
				Thread.sleep(this.clusterHeartBeatService.clusterService.config.getWaitForLeaderInterval());
			} catch (InterruptedException e) {
				logger.error("run()", e);
			}
			for ( ClusterMember member : this.clusterHeartBeatService.clusterService.getMembers() ){
				
				try {
					RunStatus stat = member.isRunning(this.clusterHeartBeatService.clusterService.config.getNetworkTimeout());
					if ( stat.isLeader() ){
						logger.info("[Leader asigning thread] Found new leader : " + member.toString());
						leaderPresent = true;
						this.clusterHeartBeatService.clusterService.leadMember = member;
						break; //found the leader
					}
				} catch (IOException e) {
					logger.error("run()", e);
				}


			}
			if (leaderPresent){
				break;
			}
		}
		if (!leaderPresent){
			//make this leader
			this.clusterHeartBeatService.clusterService.setLeader(true);
			ClusterMember member = this.clusterHeartBeatService.clusterService.findCurrent();
			member.setStartedAsLead();
			this.clusterHeartBeatService.clusterService.leadMember = member;
			logger.info("[Leader asigning thread] No leader present - Making current as leader");
		}

		this.clusterHeartBeatService.assigningNewLeader=false;

		if (logger.isDebugEnabled()) {
			logger.debug("run() - end");
		}
	}
}