package com.thinkbiganalytics.hsa.analytics;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DataLoader {

	private static Configuration conf = new Configuration();

	private static void usage() {
		System.out
				.println("usage: <loader.properties> <input> <output>");
		System.exit(1);
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException, URISyntaxException {

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 3)
			usage();
		setConfiguration(otherArgs[0]);

		if (!hbaseTableExists(LoaderConstants.MEMBER_TABLE)) {
			createTable(LoaderConstants.MEMBER_TABLE, LoaderConstants.FAMILIES);
		}
		Job job = new Job(conf, "Data Loader");
		job.setJarByClass(DataLoader.class);
		job.setMapperClass(DataLoaderMapper.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		job.waitForCompletion(true);
	}

	private static void setConfiguration(String propertiesPath) {
		LoaderProperties tutorialProperties = new LoaderProperties(
				propertiesPath);
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", tutorialProperties.getZKQuorum());
		conf.set("hbase.zookeeper.property.clientPort",
				tutorialProperties.getZKPort());
		conf.set("hbase.master", tutorialProperties.getHBMaster());
		conf.set("hbase.rootdir", tutorialProperties.getHBrootDir());
	}

	private static void createTable(String tableName, String[] families) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			if (!admin.tableExists(tableName)) {
				HTableDescriptor tableDesc = new HTableDescriptor(tableName);
				for (String family : families) {
					tableDesc.addFamily(new HColumnDescriptor(family));
				}
				admin.createTable(tableDesc);
			} else {
				System.out.println("Table " + tableName
						+ " already exists. Delete it first.");
				System.exit(0);
			}
		} catch (MasterNotRunningException e) {
			throw new RuntimeException("Unable to create the table "
					+ tableName + ". The actual exception is: "
					+ e.getMessage(), e);
		} catch (ZooKeeperConnectionException e) {
			throw new RuntimeException("Unable to create the table "
					+ tableName + ". The actual exception is: "
					+ e.getMessage(), e);
		} catch (IOException e) {
			throw new RuntimeException("Unable to create the table "
					+ tableName + ". The actual exception is: "
					+ e.getMessage(), e);
		} finally {
			close(admin);
		}
	}

	private static boolean hbaseTableExists(String tableName) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			return admin.tableExists(tableName);
		} catch (MasterNotRunningException e) {
			throw new RuntimeException(
					"Unable to check if the following table exists: "
							+ tableName + ". The actual exception is: "
							+ e.getMessage(), e);
		} catch (ZooKeeperConnectionException e) {
			throw new RuntimeException(
					"Unable to check if the following table exists: "
							+ tableName + ". The actual exception is: "
							+ e.getMessage(), e);
		} catch (IOException e) {
			throw new RuntimeException(
					"Unable to check if the following table exists: "
							+ tableName + ". The actual exception is: "
							+ e.getMessage(), e);
		} finally {
			close(admin);
		}
	}

	private static void close(Closeable c) {
		if (c == null)
			return;
		try {
			c.close();
		} catch (IOException e) {
		}
	}
}