package org.tuhin.app;

public class CommandOutput {
	private String date;
	private String command;
	private String output;
	public CommandOutput(String date, String command, String output) {
		super();
		this.date = date;
		this.command = command;
		this.output = output;
	}
	public String getDate() {
		return date;
	}
	public String getCommand() {
		return command;
	}
	public String getOutput() {
		return output;
	}
	
	

}
