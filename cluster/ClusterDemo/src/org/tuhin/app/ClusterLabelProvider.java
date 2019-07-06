package org.tuhin.app;

import java.sql.Timestamp;

import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.swt.graphics.Image;
import org.tuhin.cluster.ClusterMember;

public class ClusterLabelProvider implements ITableLabelProvider  {

	@Override
	public void addListener(ILabelProviderListener arg0) {
	}

	@Override
	public void dispose() {
	}

	@Override
	public boolean isLabelProperty(Object arg0, String arg1) {
		return false;
	}

	@Override
	public void removeListener(ILabelProviderListener arg0) {
	}

	@Override
	public Image getColumnImage(Object arg0, int arg1) {
		return null;
	}

	@Override
	public String getColumnText(Object arg0, int arg1) {
		ClusterMember node = (ClusterMember) arg0;
	    String text = "";
	    switch (arg1) {
	    case 0:
	      text = node.getId().toString() + ((node.isCurrent())?" * ":"") + ((node.getStartedAsLead() == -1)?"":" L ");
	      break;
	    case 1:
	      text = node.getAddress().getHostName() + ":" + node.getPort();
	      break;
	    case 2:
	      text = new Timestamp(node.getStarted()).toString();
	      break;
	    }
	    return text;
	}

}
