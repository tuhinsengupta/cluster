package org.tuhin.app;

import org.eclipse.swt.widgets.Table;

public class UpdatableTable {
	
	private Table table;
	private TableDataModel model;
	private boolean appendMode;
	public UpdatableTable(Table table, TableDataModel model, boolean appendMode) {
		super();
		this.table = table;
		this.model = model;
		this.appendMode = appendMode;
	}
	public Table getTable() {
		return table;
	}
	public TableDataModel getModel() {
		return model;
	}
	public boolean isAppendMode() {
		return appendMode;
	}
	
	

}
