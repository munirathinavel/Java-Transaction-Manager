package com.tricon.tm;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriconTransactionSynchronizationRegistryObjectFactory implements ObjectFactory {
	private static Logger logger = LoggerFactory.getLogger(TriconTransactionSynchronizationRegistryObjectFactory.class);

	public Object getObjectInstance(Object objRef, Name name, Context ctx, Hashtable<?, ?> env) throws Exception {
		logger.debug("Inside getObjectInstance() - objRef: {}, name: {}, ctx: {}, env: {}",
				new Object[] { objRef, name, ctx, env });

		Reference ref = (Reference) objRef;
		logger.debug("ref class name: {}", ref.getClassName());
		if (ref.getClassName().equals("com.tricon.tm.TriconTransactionSynchronizationRegistry")) {
			return TriconTransactionManagerServices.getTransactionSynchronizationRegistry();
		}
		return null;
	}

}
