package com.splicemachine.si.impl;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.*;
import com.splicemachine.si.coprocessors.SICompactionScanner;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.data.api.IHTable;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import java.io.IOException;
import java.util.Collection;

/**
 * Base implementation of a TransactionalRegion
 * @author Scott Fines
 * Date: 7/1/14
 */
public class TxnRegion implements TransactionalRegion {
		private final HRegion region;
		private final RollForward rollForward;
		private final ReadResolver readResolver;
		private final TxnSupplier txnSupplier;
		private final DataStore dataStore;
		private final Transactor transactor;
		private final IHTable hbRegion;

		private final boolean transactionalWrites; //if false, then will use straightforward writes

		public TxnRegion(HRegion region,
                     RollForward rollForward,
                     ReadResolver readResolver,
                     TxnSupplier txnSupplier,
                     DataStore dataStore,
                     Transactor transactor) {
				this.region = region;
				this.rollForward = rollForward;
				this.readResolver = readResolver;
				this.txnSupplier = txnSupplier;
				this.dataStore = dataStore;
				this.transactor = transactor;
				this.hbRegion = new HbRegion(region);

				this.transactionalWrites = SIObserver.doesTableNeedSI(region.getTableDesc().getNameAsString());
		}

		@Override
		public TxnFilter unpackedFilter(TxnView txn) throws IOException {
				return new SimpleTxnFilter(txnSupplier,txn,readResolver,dataStore);
		}

		@Override
		public TxnFilter packedFilter(TxnView txn, EntryPredicateFilter predicateFilter, boolean countStar) throws IOException {
				return new PackedTxnFilter(unpackedFilter(txn),new HRowAccumulator(dataStore.getDataLib(),predicateFilter,new EntryDecoder(),countStar));
		}

		@Override
		public DDLFilter ddlFilter(Txn ddlTxn) throws IOException {
				throw new UnsupportedOperationException("IMPLEMENT");
		}

		@Override
		public SICompactionState compactionFilter() throws IOException {
				return new SICompactionState(dataStore,txnSupplier,rollForward);
		}

		@Override
		public boolean rowInRange(byte[] row) {
				return HRegion.rowIsInRange(region.getRegionInfo(),row);
		}

		@Override
		public boolean isClosed() {
				return region.isClosed()||region.isClosing();
		}

		@Override
		public boolean containsRange(byte[] start, byte[] stop) {
				return HRegionUtil.containsRange(region, start, stop);
		}

		@Override
		public String getTableName() {
				return region.getTableDesc().getNameAsString();
		}

		@Override
		public void updateWriteRequests(long writeRequests) {
				HRegionUtil.updateWriteRequests(region, writeRequests);
		}

		@Override
		public void updateReadRequests(long readRequests) {
				HRegionUtil.updateReadRequests(region,readRequests);
		}

		@Override
		@SuppressWarnings("unchecked")
		public OperationStatus[] bulkWrite(TxnView txn,
																			 byte[] family, byte[] qualifier,
																			 ConstraintChecker constraintChecker, //TODO -sf- can we encapsulate this as well?
																			 Collection<KVPair> data) throws IOException {
					if(transactionalWrites)
							return transactor.processKvBatch(hbRegion,rollForward,txn,family,qualifier,data,constraintChecker);
					else
						return hbRegion.batchMutate(data, txn);
		}

		@Override public String getRegionName() { return region.getRegionNameAsString(); }

    @Override public TxnSupplier getTxnSupplier() { return txnSupplier; }
    @Override public ReadResolver getReadResolver() { return readResolver; }
    @Override public DataStore getDataStore() { return dataStore; }

    @Override public void discard() { } //no-op

    @Override
    public InternalScanner compactionScanner(InternalScanner scanner) {
        SICompactionState state = new SICompactionState(dataStore,txnSupplier,rollForward);
        return new SICompactionScanner(state,scanner,dataStore.getDataLib());
    }

}