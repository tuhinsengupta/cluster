package org.tuhin.app;

import java.io.Serializable;

public class PrintHelloWorld implements Runnable,Serializable {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3981183037811963615L;

	public void run() {
		ClusterDemo.addExecOutput(this.getClass().getName(), "Hello World!");
	}
}
