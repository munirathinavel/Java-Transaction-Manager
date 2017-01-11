package com.tricon.tm.internal.exception;

import javax.transaction.HeuristicCommitException;

public class TriconHeuristicCommitException extends HeuristicCommitException {

	public TriconHeuristicCommitException(String string) {
		super(string);
	}

	public TriconHeuristicCommitException(String string, Throwable t) {
		super(string);
		initCause(t);
	}

}
