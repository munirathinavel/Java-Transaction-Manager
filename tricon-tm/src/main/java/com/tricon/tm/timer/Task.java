package com.tricon.tm.timer;

public interface Task {
	Object getObject();

	void execute() throws TaskException;
}
