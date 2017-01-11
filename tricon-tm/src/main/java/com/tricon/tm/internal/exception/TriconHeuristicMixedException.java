package com.tricon.tm.internal.exception;

import javax.transaction.HeuristicMixedException;

public class TriconHeuristicMixedException extends HeuristicMixedException {

	public TriconHeuristicMixedException(String string) {
		super(string);
	}

	public TriconHeuristicMixedException(String string, Throwable t) {
		super(string);
		initCause(t);
	}

}
