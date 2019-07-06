package org.tuhin.app;

import java.util.ArrayList;
import java.util.List;

public class TableRow {
	
	private List<TableColValue> columns = new ArrayList<TableColValue>();
	
	public void add(TableColValue col) {
		columns.add(col);
	}

	public List<TableColValue> getColumns() {
		return columns;
	}
	
	

}
