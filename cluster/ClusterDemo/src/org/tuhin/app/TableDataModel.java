package org.tuhin.app;

import org.eclipse.jface.viewers.IBaseLabelProvider;
import org.eclipse.jface.viewers.IContentProvider;

public interface TableDataModel {

	public IContentProvider getProvider();

	public IBaseLabelProvider getLabelProvider();

	public TableCol[] getColumnDetails();

	public Object[][] getData();
}
