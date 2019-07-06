package org.tuhin.cluster;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

public interface CustomOperation  extends Serializable{
	
	public Object execute(Object... args) throws ExecutionException;

}
