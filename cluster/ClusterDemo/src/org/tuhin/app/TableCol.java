package org.tuhin.app;

import org.eclipse.swt.SWT;

public class TableCol {

	private String name;
	private int alignment = SWT.LEFT;

	public TableCol(String name, int alignment) {
		super();
		this.name = name;
		this.alignment = alignment;
	}

	public int getAlignment() {
		return alignment;
	}

	public void setAlignment(int alignment) {
		this.alignment = alignment;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public TableCol(String name) {
		super();
		this.name = name;
	}
	
	
}
