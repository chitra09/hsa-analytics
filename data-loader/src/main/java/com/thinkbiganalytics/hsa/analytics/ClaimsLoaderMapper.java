package com.thinkbiganalytics.hsa.analytics;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClaimsLoaderMapper extends
		Mapper<Object, Text, ImmutableBytesWritable, Put> {

	private static final Logger LOG = LoggerFactory
			.getLogger(ClaimsLoaderMapper.class);
	ImmutableBytesWritable hKey = new ImmutableBytesWritable();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			Claims claims = new Claims(value.toString());
			if (claims.isValid()) {
				// The row is either claims or claims details data

				// TODO: use salted rowkeys
				String rowkey = claims.getClaimID();

				if (claims.getType() == DataType.CLAIMS) {
					// Claims
					// NewClaimID,NewMemberID,DependentServiced,ClaimType,DateReceived,
					// DateProcessed
					// ,ServiceStart,ServiceEnd,RepricedAmount,PatientResponsibilityAmount

					Put put = new Put(Bytes.toBytes(rowkey));
					put.add(Bytes.toBytes(LoaderConstants.CLAIMS_FAMILY),
							Bytes.toBytes("mID"),
							Bytes.toBytes(claims.getMemberID()));
					put.add(Bytes.toBytes(LoaderConstants.CLAIMS_FAMILY),
							Bytes.toBytes("dependent"),
							Bytes.toBytes(claims.getDependentServiced()));
					put.add(Bytes.toBytes(LoaderConstants.CLAIMS_FAMILY),
							Bytes.toBytes("type"),
							Bytes.toBytes(claims.getClaimType()));
					put.add(Bytes.toBytes(LoaderConstants.CLAIMS_FAMILY),
							Bytes.toBytes("dtRcvd"),
							Bytes.toBytes(claims.getDateRcvd()));
					put.add(Bytes.toBytes(LoaderConstants.CLAIMS_FAMILY),
							Bytes.toBytes("dtProcessed"),
							Bytes.toBytes(claims.getDateProcessed()));
					put.add(Bytes.toBytes(LoaderConstants.CLAIMS_FAMILY),
							Bytes.toBytes("start"),
							Bytes.toBytes(claims.getServiceStart()));
					put.add(Bytes.toBytes(LoaderConstants.CLAIMS_FAMILY),
							Bytes.toBytes("end"),
							Bytes.toBytes(claims.getServiceEnd()));
					put.add(Bytes.toBytes(LoaderConstants.CLAIMS_FAMILY),
							Bytes.toBytes("repricedAmt"),
							Bytes.toBytes(claims.getRepricedAmt()));
					put.add(Bytes.toBytes(LoaderConstants.CLAIMS_FAMILY),
							Bytes.toBytes("amt"),
							Bytes.toBytes(claims.getFinalAmt()));
					
					hKey.set(Bytes.toBytes(rowkey));
					context.write(hKey, put);

				} else if (claims.getType() == DataType.CLAIMS_DETAILS) {

					// Claims Details
					// NewClaimID,CPTCode
					Put put = new Put(Bytes.toBytes(rowkey));
					put.add(Bytes.toBytes(LoaderConstants.CLAIMS_FAMILY),
							Bytes.toBytes("cpt"),
							Bytes.toBytes(claims.getCptCode()));
					hKey.set(Bytes.toBytes(rowkey));
					context.write(hKey, put);
				}
			}
		} catch (NumberFormatException e) {
			LOG.error("Error in row:" + value.toString());
			LOG.error(e.getMessage());
		}
	}

}