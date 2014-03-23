package com.thinkbiganalytics.hsa.analytics;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class LoaderProperties {

	private static Properties props;
	private String HBASE_ZOOKEEPER_QUORUM;
	private String HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT;
	private String HBASE_MASTER;
	private String HBASE_ROOTDIR;

	public LoaderProperties(String propLocation) {
		props = new Properties();
		try {
			File file = new File(propLocation);
			System.out.println("Loading properties from "
					+ file.getAbsolutePath());
			FileReader is = new FileReader(file);
			props.load(is);
			System.out.println("Properties: " + props.toString());

			HBASE_ZOOKEEPER_QUORUM = props
					.getProperty("HBASE_ZOOKEEPER_QUORUM");
			HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT = props
					.getProperty("HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT");
			HBASE_MASTER = props.getProperty("HBASE_MASTER");
			HBASE_ROOTDIR = props.getProperty("HBASE_ROOTDIR");

		} catch (IOException e) {
			throw new RuntimeException("Could not load properties file");
		} catch (NullPointerException e) {
			throw new RuntimeException("File does not exist");
		}
	}

	public String getZKQuorum() {
		return HBASE_ZOOKEEPER_QUORUM;
	}

	public String getZKPort() {
		return HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT;
	}

	public String getHBMaster() {
		return HBASE_MASTER;
	}

	public String getHBrootDir() {
		return HBASE_ROOTDIR;
	}
}
