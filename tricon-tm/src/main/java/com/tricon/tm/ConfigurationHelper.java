package com.tricon.tm;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationHelper {
	private static Logger logger = LoggerFactory.getLogger(ConfigurationHelper.class);

	private static final String TRICON_TM_CONFIG_SYSTEM_PROP = "tricon.tm.configuration";
	private static final String TRICON_TM_DEFAULT_CONFIG_PROP_FILE = "/com/tricon/tm/config/tricon-tm-default-config.properties";
	private static final String TM_VENDOR_NAME = "Tricon";
	private static final String DEFAULT_SERVER_ID = "localhost";
	private static final int MAX_SERVER_ID_LENGTH = 42;
	private static final int DEFAULT_TX_TIMEOUT = 60;

	private String serverId;
	private int defaultTransactionTimeout;
	private boolean asynchronous2pc;
	private boolean useTMJoin;
	private int shutdownInterval;

	private byte[] tmVendorNameByteArray;
	private byte[] serverIdByteArray;

	protected ConfigurationHelper() {
		logger.debug("Inside ConfigurationHelper constructor - loading transaction configurations{}", "..");
		try {
			final Properties properties = loadProperties();
			logLoadedConfigurations(properties);

			serverId = getString(properties, "tricon.tm.serverId", "");
			defaultTransactionTimeout = getInt(properties, "tricon.tm.defaultTransactionTimeout", DEFAULT_TX_TIMEOUT);
			asynchronous2pc = getBoolean(properties, "tricon.tm.isAsynchronous2pc", false);
			useTMJoin = getBoolean(properties, "tricon.tm.useTMJoin", false);
			shutdownInterval = getInt(properties, "tricon.tm.shutdown.interval", 10);
			logger.debug("Loaded transaction configurations{}", ".");

		} catch (Exception ex) {
			logger.error("Error while loading transaction configurations: ", ex);
			throw new InitializationException("Error while loading transaction configurations", ex);
		}
	}

	private static Properties loadProperties() throws IOException {
		final Properties properties = new Properties();

		String configFile = System.getProperty(TRICON_TM_CONFIG_SYSTEM_PROP, null);
		InputStream inputStream = null;
		try {
			if (configFile != null) {
				logger.debug("Loading transaction configurations from file: " + configFile);
				inputStream = new FileInputStream(configFile);
			} else {
				logger.debug("Loading default transaction configurations", "..");
				inputStream = ConfigurationHelper.class.getResourceAsStream(TRICON_TM_DEFAULT_CONFIG_PROP_FILE);
			}
			properties.load(inputStream);
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
		return properties;
	}

	private static void logLoadedConfigurations(final Properties properties) {
		logger.debug("Configurations: ", "");
		final Set<Entry<Object, Object>> propEntrySet = properties.entrySet();
		for (final Entry<Object, Object> propEntry : propEntrySet) {
			logger.debug("  " + (String) propEntry.getKey() + "=" + (String) propEntry.getValue());
		}
	}

	private static String getString(Properties properties, String key, String defaultValue) {
		String value = properties.getProperty(key);
		return (value == null) ? defaultValue : value;
	}

	private static boolean getBoolean(Properties properties, String key, boolean defaultValue) {
		return Boolean.valueOf(getString(properties, key, "" + defaultValue)).booleanValue();
	}

	private static int getInt(Properties properties, String key, int defaultValue) {
		return Integer.parseInt(getString(properties, key, "" + defaultValue));
	}

	public int getDefaultTransactionTimeout() {
		return defaultTransactionTimeout;
	}

	public String getTMVendorName() {
		return TM_VENDOR_NAME;
	}

	public String getServerId() {
		return DEFAULT_SERVER_ID;
	}

	public boolean isAsynchronous2pc() {
		return asynchronous2pc;
	}

	public boolean isUseTMJoin() {
		return useTMJoin;
	}

	public int getShutdownInterval() {
		return shutdownInterval;
	}

	public byte[] buildTMVendorNameByteArray() {
		// 6 bytes
		if (tmVendorNameByteArray == null) {
			tmVendorNameByteArray = TM_VENDOR_NAME.getBytes();
		}
		return tmVendorNameByteArray;
	}

	public byte[] buildServerIdByteArray() {
		// 42 bytes
		if (serverIdByteArray == null) {
			try {
				serverIdByteArray = serverId.substring(0, Math.min(serverId.length(), MAX_SERVER_ID_LENGTH)).getBytes("US-ASCII");
			} catch (Exception ex) {
				try {
					serverIdByteArray = InetAddress.getLocalHost().getHostAddress().getBytes("US-ASCII");
				} catch (Exception ex2) {
					serverIdByteArray = DEFAULT_SERVER_ID.getBytes();
				}
			}

			if (serverIdByteArray.length > MAX_SERVER_ID_LENGTH) {
				byte[] truncatedServerId = new byte[MAX_SERVER_ID_LENGTH];
				System.arraycopy(serverIdByteArray, 0, truncatedServerId, 0, MAX_SERVER_ID_LENGTH);
				serverIdByteArray = truncatedServerId;
			}

			String serverIdArrayAsString = new String(serverIdByteArray);
			if (serverId == null || serverId.length() < 1) {
				serverId = serverIdArrayAsString;
			}
		}
		return serverIdByteArray;
	}

}
