package com.thinkbiganalytics.hsa.analytics;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataLoaderMapper extends Mapper<Object, Text, Text, IntWritable> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		HTable hbaseTable = null;
		try {
			Member member = new Member(value.toString());
			if (member.isValid()) {
				// The row is either member or dependent data

				// TODO: use salted rowkeys
				String rowkey = member.getdID();

				hbaseTable = new HTable(context.getConfiguration(),
						Bytes.toBytes(LoaderConstants.MEMBER_TABLE));

				if (member.isMember()) {
					// Member
					// NewMemberID,State,Zip,Gender,BirthYear,HsaEffectiveDate

					Put put = new Put(Bytes.toBytes(rowkey));
					put.add(Bytes.toBytes(LoaderConstants.MEMBER_FAMILY),
							Bytes.toBytes("state"),
							Bytes.toBytes(member.getState()));
					put.add(Bytes.toBytes(LoaderConstants.MEMBER_FAMILY),
							Bytes.toBytes("zip"),
							Bytes.toBytes(member.getZip()));
					put.add(Bytes.toBytes(LoaderConstants.MEMBER_FAMILY),
							Bytes.toBytes("g"),
							Bytes.toBytes(member.getGender()));
					put.add(Bytes.toBytes(LoaderConstants.MEMBER_FAMILY),
							Bytes.toBytes("year"),
							Bytes.toBytes(member.getGender()));
					put.add(Bytes.toBytes(LoaderConstants.MEMBER_FAMILY),
							Bytes.toBytes("date"),
							Bytes.toBytes(member.getGender()));
					hbaseTable.put(put);
				} else {
					// Dependent
					// NewMemberID,DependentID,Relationship,BirthYear,Gender,State,Zip

					Put put = new Put(Bytes.toBytes(rowkey));
					put.add(Bytes.toBytes(LoaderConstants.DEPENDENT_FAMILY),
							Bytes.toBytes("dID"),
							Bytes.toBytes(member.getdID()));
					put.add(Bytes.toBytes(LoaderConstants.DEPENDENT_FAMILY),
							Bytes.toBytes("dRel"),
							Bytes.toBytes(member.getRelationship()));
					put.add(Bytes.toBytes(LoaderConstants.DEPENDENT_FAMILY),
							Bytes.toBytes("dYear"),
							Bytes.toBytes(member.getdBirthYear()));
					put.add(Bytes.toBytes(LoaderConstants.DEPENDENT_FAMILY),
							Bytes.toBytes("dG"),
							Bytes.toBytes(member.getdGender()));
					put.add(Bytes.toBytes(LoaderConstants.DEPENDENT_FAMILY),
							Bytes.toBytes("dState"),
							Bytes.toBytes(member.getdState()));
					put.add(Bytes.toBytes(LoaderConstants.DEPENDENT_FAMILY),
							Bytes.toBytes("dZip"),
							Bytes.toBytes(member.getdZip()));
					hbaseTable.put(put);
				}

			}
		} finally {
			close(hbaseTable);
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