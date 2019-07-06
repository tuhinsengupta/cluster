package org.tuhin.cluster;
import java.io.Serializable;


public class ResultObject implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -847639355439268441L;
	private Object result;
	private Throwable exception;
	public ResultObject(Object result, Throwable exception) {
		super();
		this.result = result;
		this.exception = exception;
	}
	public ResultObject(Throwable e) {
		this(null,e);
	}
	public ResultObject(Object result) {
		this(result,null);
	}
	public Object getResult() {
		return result;
	}
	public Throwable getException() {
		return exception;
	}

	public boolean isSuccess(){
		return (exception == null );
	}
	

}
