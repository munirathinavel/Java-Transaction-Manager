package com.tricon.tm.timer;

import java.util.Date;

public abstract class ScheduledTask implements Task {
	private Date executionTime;
	
	protected ScheduledTask(Date executionTime){
		this.executionTime=executionTime;
	}
	
	public void setExecutionTime(Date executionTime) {
		this.executionTime = executionTime;
	}

	public Date getExecutionTime() {
		return executionTime;
	}
	
	public abstract Object getObject();

	public abstract void execute() throws TaskException;

}
