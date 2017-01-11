package com.tricon.tm.twopc;

import java.util.LinkedHashMap;
import java.util.Map;

public class PhaseException extends Exception {
	Map resourceExceptionMap = new LinkedHashMap();

	public PhaseException(Map resourceExceptionMap) {
		this.resourceExceptionMap = resourceExceptionMap;
	}

	public Map getResourceExceptionMap() {
		return resourceExceptionMap;
	}

}
