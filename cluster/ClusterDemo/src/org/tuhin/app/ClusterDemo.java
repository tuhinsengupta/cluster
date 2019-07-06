package org.tuhin.app;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.eclipse.jface.viewers.IBaseLabelProvider;
import org.eclipse.jface.viewers.IContentProvider;
import org.eclipse.swt.SWT;
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
			public IContentProvider getProvider() {
				return new ClusterContentProvider();
			}
			public IBaseLabelProvider getLabelProvider() {
				return new ClusterLabelProvider();
			}
			@Override
			public TableCol[] getColumnDetails() {
				
				return new TableCol[]{new TableCol("Node"), new TableCol("Address"), new TableCol("Started At", SWT.RIGHT)};
			}
			@Override
			public Object[][] getData() {
				Object[][] data = new Object[cluster.getNodes().size()][3];
				int index_r = 0;
				for( ClusterMember node: cluster.getNodes()) {
					data[index_r][0] = node.getId().toString() + ((node.isCurrent())?" * ":"") + ((node.getStartedAsLead() == -1)?"":" L ");
					data[index_r][1] = node.getAddress().getHostName() + ":" + node.getPort();
					data[index_r][2] = new Timestamp(node.getStarted()).toString();
					index_r++;
				}
				return data;
			}
		});
		
	}

	private void createTable(Composite composite, TableDataModel model) {
		
		Table table = new Table(composite, SWT.FULL_SELECTION | SWT.BORDER | SWT.V_SCROLL| SWT.H_SCROLL);
		table.setHeaderVisible(true);
	    table.setLinesVisible(true);

	    for (TableCol col: model.getColumnDetails() ) {
	    	TableColumn tc = new TableColumn(table, col.getAlignment());
	    	tc.setText(col.getName());
	    }
	    
	    for( Object[] row: model.getData()) {
	    	TableItem item = new TableItem(table, SWT.NULL);
	    	int i=0;
	    	for(Object col : row) {
	    		item.setText(i++, (String)col);
	    	}
	    	
	    }
	    
	    for (int loopIndex = 0; loopIndex < model.getColumnDetails().length; loopIndex++) {
	        table.getColumn(loopIndex).pack();
	    }
	    
	    updateList.add(new UpdatableTable(table,model));
	}
	
	private void updateData() {
		for(UpdatableTable updatableTable: updateList ) {
			TableDataModel model = updatableTable.getModel();
			Table table = updatableTable.getTable();
			
			table.removeAll();
		    
		    for( Object[] row: model.getData()) {
		    	TableItem item = new TableItem(table, SWT.NULL);
		    	int i=0;
		    	for(Object col : row) {
		    		item.setText(i++, (String)col);
		    	}
		    	
		    }
		    
		    for (int loopIndex = 0; loopIndex < model.getColumnDetails().length; loopIndex++) {
		        table.getColumn(loopIndex).pack();
		    }
		}
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		
		shell.setText("Cluster Group : " + cluster.getName() + "[Last Updated : " + dateFormat.format(date) + "]");
	}

}
