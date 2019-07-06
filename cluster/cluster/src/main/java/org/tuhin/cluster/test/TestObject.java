package org.tuhin.cluster.test;
import java.io.Serializable;


public class TestObject implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4137592615574083905L;
	private String name;

	public TestObject(String name) {
		super();
		this.name = name;
	}

	@Override
	public String toString() {
		return "TestObject [name=" + name + "]";
	}
	
	

}
