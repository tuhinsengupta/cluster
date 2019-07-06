package org.tuhin.cluster.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

public class CollectionUtils {
	
	public static  <T> List<Collection<? extends Callable<T>>> split(Collection<? extends Callable<T>> coll, int noOfLists){
		
		List<Collection<? extends Callable<T>>> retVal = new ArrayList<Collection<? extends Callable<T>>>();
		
		int size = coll.size();
		
		
		int maxElements = size / noOfLists;
		
		int remainder = size % noOfLists;
		
		if ( remainder > 0 ){
			maxElements++;
		}

		Iterator<? extends Callable<T>> iter = coll.iterator();
		for(int i=0 ; i < noOfLists; i++){
			Collection<Callable<T>> newColl = new ArrayList<Callable<T>>();
			for(int j=0;j<maxElements; j++){
				if ( iter.hasNext()){
					newColl.add(iter.next());
				}
			}
			retVal.add(newColl);
		}
		
		return retVal;
		
	}
	

}
