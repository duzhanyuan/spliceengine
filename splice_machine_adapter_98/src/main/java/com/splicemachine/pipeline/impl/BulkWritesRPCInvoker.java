package com.splicemachine.pipeline.impl;

import com.splicemachine.derby.hbase.SpliceBaseIndexEndpoint;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.pipeline.api.BulkWritesInvoker;
import org.apache.hadoop.hbase.client.HConnection;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/31/14
 */
public class BulkWritesRPCInvoker implements BulkWritesInvoker {

    private SpliceIndexEndpointRPCClient indexEndpointRPC;

    public BulkWritesRPCInvoker(HConnection connection, byte[] tableName) {
        this.indexEndpointRPC = new SpliceIndexEndpointRPCClient(connection, tableName);
    }

    @Override
    public BulkWritesResult invoke(final BulkWrites writes, boolean refreshCache) throws IOException {
        assert writes.numEntries() != 0;

        BulkWrite firstBulkWrite = (BulkWrite) writes.getBuffer()[0];
        String encodedRegionName = firstBulkWrite.getEncodedStringName();
        SpliceBaseIndexEndpoint indexEndpoint = SpliceDriver.driver().getSpliceIndexEndpoint(encodedRegionName);

        if (indexEndpoint != null) {
            return indexEndpoint.bulkWrite(writes);
        }

        return indexEndpointRPC.bulkWrite(writes);
    }


    public static final class Factory implements BulkWritesInvoker.Factory {
        private final HConnection connection;
        private final byte[] tableName;

        public Factory(HConnection connection, byte[] tableName) {
            this.connection = connection;
            this.tableName = tableName;
        }

        @Override
        public BulkWritesInvoker newInstance() {
            return new BulkWritesRPCInvoker(connection, tableName);
        }
    }
}