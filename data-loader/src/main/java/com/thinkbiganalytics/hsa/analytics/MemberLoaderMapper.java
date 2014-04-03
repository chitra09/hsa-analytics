package com.thinkbiganalytics.hsa.analytics;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemberLoaderMapper extends
		Mapper<Object, Text, ImmutableBytesWritable, Put> {

	private static final Logger LOG = LoggerFactory
			.getLogger(MemberLoaderMapper.class);
	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			Member member = new Member(value.toString());
			if (member.isValid()) {
				// The row is either member or dependent data

				// TODO: use salted rowkeys
				String rowkey = member.getMemberID() + "_" + member.getdID();

				if (!rowkey.isEmpty()) {
					if (member.getType() == DataType.MEMBER) {
						// Member
						// NewMemberID,State,Zip,Gender,BirthYear,HsaEffectiveDate
						// Dependent
						// NewMemberID,DependentID,Relationship,BirthYear,Gender,State,Zip

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
								Bytes.toBytes("birthYr"),
								Bytes.toBytes(member.getBirthYear()));
						put.add(Bytes.toBytes(LoaderConstants.MEMBER_FAMILY),
								Bytes.toBytes("effectiveDt"),
								Bytes.toBytes(member.getHsaEffectiveDate()));
						put.add(Bytes.toBytes(LoaderConstants.DEPENDENT_FAMILY),
								Bytes.toBytes("dID"),
								Bytes.toBytes(member.getdID()));
						put.add(Bytes.toBytes(LoaderConstants.DEPENDENT_FAMILY),
								Bytes.toBytes("dRel"),
								Bytes.toBytes(member.getRelationship()));
						put.add(Bytes.toBytes(LoaderConstants.DEPENDENT_FAMILY),
								Bytes.toBytes("dBirthYr"),
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
						hKey.set(Bytes.toBytes(rowkey));
						context.write(hKey, put);
					} 
				}
			}
		} catch (NumberFormatException e) {
			LOG.error("Error in row:" + value.toString());
			LOG.error(e.getMessage());
		}
	}
	
	/*
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			Member member = new Member(value.toString());
			if (member.isValid()) {
				// The row is either member or dependent data

				// TODO: use salted rowkeys
				String rowkey = member.getMemberID() + "_" + member.getdID();

				if (!rowkey.isEmpty()) {
					if (member.getType() == DataType.MEMBER) {
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
								Bytes.toBytes("birthYr"),
								Bytes.toBytes(member.getBirthYear()));
						put.add(Bytes.toBytes(LoaderConstants.MEMBER_FAMILY),
								Bytes.toBytes("effectiveDt"),
								Bytes.toBytes(member.getHsaEffectiveDate()));
						hKey.set(Bytes.toBytes(rowkey));
						context.write(hKey, put);

					} else if (member.getType() == DataType.DEPENDENT) {

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
								Bytes.toBytes("dBirthYr"),
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
						hKey.set(Bytes.toBytes(rowkey));
						context.write(hKey, put);
					} 
				}
			}
		} catch (NumberFormatException e) {
			LOG.error("Error in row:" + value.toString());
			LOG.error(e.getMessage());
		}
	}
	*/

}