package com.tricon.tm.resource.jdbc;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;

import oracle.jdbc.xa.client.OracleXADataSource;

import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.managed.DataSourceXAConnectionFactory;
import org.apache.commons.dbcp.managed.ManagedDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tricon.tm.TriconTransactionManager;
import com.tricon.tm.TriconTransactionManagerServices;

public class DBCPDataSourceFactory implements ObjectFactory {
	private static Logger logger = LoggerFactory.getLogger(DBCPDataSourceFactory.class);

	private final Hashtable<String, DataSource> dataSources = new Hashtable<String, DataSource>();

	private final TriconTransactionManager transactionManager = TriconTransactionManagerServices.getTransactionManager();

	public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> env) throws Exception {
		logger.debug("Inside getObjectInstance() - obj: {}, name: {}, nameCtx: {}, env: {}",
				new Object[] { obj, name, nameCtx, env });

		final Reference ref = (Reference) obj;
		String uniqueName = getUniqueName(ref);
		if (!dataSources.containsKey(uniqueName)) {
			dataSources.put(uniqueName, createPooledXADataSource(ref));
		}
		return dataSources.get(uniqueName);
	}

	private synchronized String getUniqueName(final Reference ref) throws Exception {
		RefAddr refAddr = ref.get("uniqueName");
		if (refAddr == null) {
			throw new NamingException("No 'uniqueName' RefAddr found!");
		}
		return (String) refAddr.getContent();
	}

	private synchronized ManagedDataSource createPooledXADataSource(final Reference ref) throws Exception {
		final Map<String, String> resourceConfigMap = loadResourceConfigurations(ref);

		// Note:- We can create XADataSource for different database type also based on dynamic resource parameter.
		final OracleXADataSource xaDataSource = createOracleXADataSource(resourceConfigMap);

		DataSourceXAConnectionFactory connFactory =
				new DataSourceXAConnectionFactory(transactionManager, xaDataSource);

		GenericObjectPool pool = new GenericObjectPool();
		pool.setMaxActive(Integer.parseInt(resourceConfigMap.get("maxActive")));
		PoolableConnectionFactory factory = new PoolableConnectionFactory(connFactory,
				pool, null, null, false, true);
		pool.setFactory(factory);
		ManagedDataSource pooledXADataSource = new ManagedDataSource(pool,
				connFactory.getTransactionRegistry());

		logger.debug("Inside createPooledXADataSource() - uniqueName: {}, dataSource: {}",
				xaDataSource.getDataSourceName(), pooledXADataSource);
		return pooledXADataSource;
	}

	private synchronized Map<String, String> loadResourceConfigurations(final Reference ref) {
		final Map<String, String> resourceConfigMap = new HashMap<String, String>();
		final Enumeration<RefAddr> addrs = ref.getAll();
		while (addrs.hasMoreElements()) {
			RefAddr addr = (RefAddr) addrs.nextElement();
			String propName = addr.getType();
			String value = (String) addr.getContent();
			if (propName.equals("driverClassName")) {
				resourceConfigMap.put(propName, value);
			} else if (propName.equals("url")) {
				resourceConfigMap.put(propName, value);
			} else if (propName.equals("username")) {
				resourceConfigMap.put(propName, value);
			} else if (propName.equals("password")) {
				resourceConfigMap.put(propName, value);
			} else if (propName.equals("uniqueName")) {
				resourceConfigMap.put(propName, value);
			} else if (propName.equals("maxActive")) {
				resourceConfigMap.put(propName, value);
			}
		}
		return resourceConfigMap;
	}

	private synchronized OracleXADataSource createOracleXADataSource(final Map<String, String> resourceConfigMap)
			throws Exception {
		final OracleXADataSource ds = new OracleXADataSource();
		ds.setDriverType(resourceConfigMap.get("driverClassName"));
		ds.setURL(resourceConfigMap.get("url"));
		ds.setUser(resourceConfigMap.get("username"));
		ds.setPassword(resourceConfigMap.get("password"));
		ds.setDataSourceName(resourceConfigMap.get("uniqueName"));
		return ds;
	}

}
