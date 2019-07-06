package org.tuhin.app;

import java.util.ArrayList;
import java.util.List;


public class TableData {
	
	private List<TableRow> rows = new ArrayList<TableRow>();
	
	public void add(TableRow row) {
		rows.add(row);
	}

	public List<TableRow> getRows() {
		return rows;
	}
	

}
