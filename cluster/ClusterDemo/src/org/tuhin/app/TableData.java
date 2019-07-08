package org.tuhin.app;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class TableData {
	
	private List<TableRow> rows = new ArrayList<TableRow>();
	private Comparator<TableRow> compareFunc = null;
	
	public TableData(int sortingCol) {
		compareFunc = new Comparator<TableRow>() {
			
			@Override
			public int compare(TableRow o1, TableRow o2) {
				String t1 = o1.getColumns().get(sortingCol).getValue();
				String t2 = o2.getColumns().get(sortingCol).getValue();
				return t1.compareTo(t2);
			}
		};
		
	}
	
	public void add(TableRow row) {
		rows.add(row);
	}

	
	public List<TableRow> getRows() {
		if (compareFunc != null) {
		 Collections.sort(rows,compareFunc);
		}
		return rows;
	}
	

}
