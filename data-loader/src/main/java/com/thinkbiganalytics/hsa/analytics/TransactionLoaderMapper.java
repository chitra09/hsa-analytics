package com.thinkbiganalytics.hsa.analytics;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionLoaderMapper extends
		Mapper<Object, Text, ImmutableBytesWritable, Put> {

	private static final Logger LOG = LoggerFactory
			.getLogger(TransactionLoaderMapper.class);
	ImmutableBytesWritable hKey = new ImmutableBytesWritable();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			Member member = new Member(value.toString());
			if (member.isValid()) {
				// The row is transaction data

				// TODO: use salted rowkeys
				String rowkey = member.getMemberID() + "_"
						+ member.getCategory() + "_"
						+ member.getPaymentAvailableDate();
				if (member.getType() == DataType.TRANSACTION) {

					// Transaction
					// NewMemberID,Amount,Category,PaymentAvailableDate

					Put put = new Put(Bytes.toBytes(rowkey));
					put.add(Bytes.toBytes(LoaderConstants.TRANSACTION_FAMILY),
							Bytes.toBytes("amt"),
							Bytes.toBytes(member.getAmount()));
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