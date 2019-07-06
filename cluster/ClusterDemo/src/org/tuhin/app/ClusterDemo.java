package org.tuhin.app;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.tuhin.cluster.ClusterMember;

public class ClusterDemo {

	
	protected Shell shell;

	private List<UpdatableTable> updateList = new ArrayList<UpdatableTable>();

	private static Cluster cluster;
	/**
	 * Launch the application.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			cluster = Cluster.getInstance();
			ClusterDemo window = new ClusterDemo();
			window.open();
			cluster.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Open the window.
	 */
	public void open() {
		Display.setAppName("Cluster Demo");
		Display display = Display.getDefault();
		createContents();
		shell.open();
		shell.setMaximized(true);
		shell.setLayout(new FillLayout(SWT.VERTICAL));
	
		shell.layout();
		
		Thread updateThread = new Thread() {
	        public void run() {
	            while (true) {
	                Display.getDefault().syncExec(new Runnable() {

	                    @Override
	                    public void run() {
	                        updateData();
	                    }

	                });

	                try {
	                    Thread.sleep(1000);
	                } catch (InterruptedException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	    };
	    // background thread
	    updateThread.setDaemon(true);
	    updateThread.start();

		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
	}

	/**
	 * Create contents of the window.
	 */
	protected void createContents() {
		shell = new Shell();
		shell.setSize(898, 561);
		shell.setText("Cluster Group : " + cluster.getName());

		Composite composite = new Composite(shell, SWT.BORDER);
		composite.setLayout(new FillLayout(SWT.HORIZONTAL));
		
		Composite node_composite = new Composite(composite, SWT.BORDER);
		node_composite.setLayout(new FillLayout(SWT.HORIZONTAL));
		
		Composite maps_composite = new Composite(composite, SWT.BORDER);
		maps_composite.setLayout(new FillLayout(SWT.HORIZONTAL));
		
		
		Composite exec_composite = new Composite(shell, SWT.BORDER);
		exec_composite.setLayout(new FillLayout(SWT.HORIZONTAL));

		createTable(node_composite, new TableDataModel() { 
			@Override
			public TableCol[] getColumnDetails() {
				
				return new TableCol[]{new TableCol("Node"), new TableCol("Address"), new TableCol("Started At", SWT.RIGHT)};
			}
			@Override
			public TableData getData() {
				TableData data = new TableData();
				for( ClusterMember node: cluster.getNodes()) {
					TableRow row = new TableRow();
					Color fg_color = null;
					if ( node.getStartedAsLead() != -1) {
						fg_color = new Color (Display.getCurrent(), 255, 0, 0); 
					}
					Color bg_color = null;
					if ( node.isCurrent()) {
						bg_color = new Color (Display.getCurrent(), 255, 255, 153); 
					}
					row.add(new TableColValue(node.getId().toString(), fg_color, bg_color));
					row.add(new TableColValue(node.getAddress().getHostName() + ":" + node.getPort()));
					row.add(new TableColValue(new Timestamp(node.getStarted()).toString()));
					data.add(row);
				}
				return data;
			}
		});
		
	}

	private void createTable(Composite composite, TableDataModel model) {
		
		Table table = new Table(composite, SWT.FULL_SELECTION | SWT.BORDER | SWT.V_SCROLL| SWT.H_SCROLL);
		table.setHeaderVisible(true);
	    table.setLinesVisible(true);

    	TableColumn indexc = new TableColumn(table, SWT.RIGHT);
    	indexc.setText("#");

	    for (TableCol col: model.getColumnDetails() ) {
	    	TableColumn tc = new TableColumn(table, col.getAlignment());
	    	tc.setText(col.getName());
	    }

    	addItemsToTable(table,model);

	    	    
	    updateList.add(new UpdatableTable(table,model));
	}
	
	private void addItemsToTable(Table table, TableDataModel model) {
		int rowNum = 1;
	    for( TableRow row: model.getData().getRows()) {
	    	TableItem item = new TableItem(table, SWT.NULL);
	    	int i=0;
    		item.setText(i++, ""+rowNum++);
	    	for(TableColValue col : row.getColumns()) {
	    		
	    		item.setText(i++, col.getValue());
	    		if (col.getDisplayColor() != null) {
	    			item.setForeground(col.getDisplayColor());
	    		}
	    		if (col.getBackColor() != null) {
	    			item.setBackground(col.getBackColor());
	    		}
	    	}
	    	
	    }
	    for (int loopIndex = 0; loopIndex <= model.getColumnDetails().length; loopIndex++) {
	        table.getColumn(loopIndex).pack();
	    }
	}

	private void updateData() {
		for(UpdatableTable updatableTable: updateList ) {
			TableDataModel model = updatableTable.getModel();
			Table table = updatableTable.getTable();
			
			table.removeAll();
		    
			addItemsToTable(table,model);

		}
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		
		shell.setText("Cluster Group : " + cluster.getName() + "[Last Updated : " + dateFormat.format(date) + "]");
	}

}
