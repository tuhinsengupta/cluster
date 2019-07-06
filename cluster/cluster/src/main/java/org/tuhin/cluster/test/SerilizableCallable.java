package org.tuhin.cluster.test;
import java.io.Serializable;
import java.util.concurrent.Callable;


public interface SerilizableCallable<T> extends Callable<T>, Serializable {

}
