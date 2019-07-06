package org.tuhin.app;

import org.eclipse.jface.viewers.IStructuredContentProvider;

public class ClusterContentProvider implements IStructuredContentProvider{

	@Override
	public Object[] getElements(Object arg0) {
		return ((Cluster) arg0).getNodes().toArray();
	}

}
	