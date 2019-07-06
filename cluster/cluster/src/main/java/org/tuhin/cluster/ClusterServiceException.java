package org.tuhin.cluster;


public class ClusterServiceException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7036981261913465261L;

	public ClusterServiceException(Exception exception) {
		super(exception);
	}

	public ClusterServiceException(String string) {
		super(string);
	}

}
