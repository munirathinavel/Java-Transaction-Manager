package com.tricon.tm;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriconTransactionManagerObjectFactory implements ObjectFactory {
	private static Logger logger = LoggerFactory.getLogger(TriconTransactionManagerObjectFactory.class);

	public Object getObjectInstance(Object objRef, Name name, Context ctx, Hashtable<?, ?> env) throws Exception {
		logger.debug("Inside getObjectInstance() - objRef: {}, name: {}, ctx: {}, env: {}",
				new Object[] { objRef, name, ctx, env });

		Reference ref = (Reference) objRef;
		logger.debug("ref class name: {}", ref.getClassName());
		if (ref.getClassName().equals("javax.transaction.UserTransaction")) {
			return TriconTransactionManagerServices.getUserTransaction();

		} else if (ref.getClassName().equals("com.tricon.tm.TriconTransactionManager")) {
			return TriconTransactionManagerServices.getTransactionManager();
		}
		return null;
	}

}
