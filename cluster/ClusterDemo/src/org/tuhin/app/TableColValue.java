package org.tuhin.app;

import org.eclipse.swt.graphics.Color;

public class TableColValue {
	
	private String value;
	private Color displayColor = null;
	private Color backColor = null;
	public Color getBackColor() {
		return backColor;
	}

	public String getValue() {
		return value;
	}

	public Color getDisplayColor() {
		return displayColor;
	}

	public TableColValue(String value, Color color, Color bg_color) {
		super();
		this.value = value;
		this.displayColor = color;
		this.backColor = bg_color;
	}
	public TableColValue(String value) {
		super();
		this.value = value;
	}
	
	

}
