package org.tuhin.app;

import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.widgets.Table;
import org.tuhin.cluster.DistributedThreadPool;

public class RunHelloWorld implements SelectionListener{

	private DistributedThreadPool cluster_pool;
	public RunHelloWorld(DistributedThreadPool cluster_pool) {
		this.cluster_pool = cluster_pool;
	}
	@Override
	public void widgetDefaultSelected(SelectionEvent arg0) {
		
	}	
				

	@Override
	public void widgetSelected(SelectionEvent arg0) {
		cluster_pool.submit(new PrintHelloWorld());
	}

}
