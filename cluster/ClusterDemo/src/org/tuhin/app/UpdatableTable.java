package org.tuhin.app;

import org.eclipse.swt.widgets.Table;

public class UpdatableTable {
	
	private Table table;
	private TableDataModel model;
	public UpdatableTable(Table table, TableDataModel model) {
		super();
		this.table = table;
		this.model = model;
	}
	public Table getTable() {
		return table;
	}
	public TableDataModel getModel() {
		return model;
	}
	
	

}
