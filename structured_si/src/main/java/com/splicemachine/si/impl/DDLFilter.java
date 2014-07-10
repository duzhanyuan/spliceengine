package com.splicemachine.si.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class DDLFilter implements Comparable<DDLFilter> {
    private final Txn myTransaction;
//    private final TransactionStore transactionStore;
		private final TxnSupplier transactionStore;
		private Cache<Long,Boolean> visibilityMap;
//    private ConcurrentMap<String, Boolean> visibilityMap;

		public DDLFilter( Txn myTransaction, TxnSupplier transactionStore) {
				this.myTransaction = myTransaction;
				this.transactionStore = transactionStore;
				visibilityMap = CacheBuilder.newBuilder().expireAfterWrite(60, TimeUnit.SECONDS).maximumSize(10000).build();
		}

		public boolean isVisibleBy(final Txn txn) throws IOException {
        Boolean visible = visibilityMap.getIfPresent(txn.getTxnId());
        if(visible!=null) return visible;

        //if I haven't succeeded yet, don't do anything
        if(myTransaction.getEffectiveState()!= Txn.State.COMMITTED) return false;

        Txn parentTxn = myTransaction.getParentTransaction();
        //if I have a parent, and he was rolled back, don't do anything
        if(parentTxn.getEffectiveState()== Txn.State.ROLLEDBACK) return false;
        try{
            return visibilityMap.get(txn.getTxnId(),new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return isVisible(txn);
                }
            });
        }catch(ExecutionException ee){
            throw new IOException(ee.getCause());
        }

		}

    private Boolean isVisible(Txn txn) {
				/*
				 * For the purposes of DDL, we intercept any writes which occur AFTER us, regardless of
				 * my status.
				 *
				 * The reason for this is because the READ of us will not see those writes, which means
				 * that we need to intercept them and deal with them as if we were committed. If we rollback,
				 * then it shouldn't matter to the other operation (except for performance), and if we commit,
				 * then we should see properly constructed data.
				 */
        long otherTxnId = txn.getTxnId();
        return myTransaction.getTxnId()<=otherTxnId;
    }

    public boolean isVisibleBy(final long txnId) throws IOException{
        Boolean visible = visibilityMap.getIfPresent(txnId);
        if (visible != null) {
            return visible;
        }
        //if I haven't succeeded yet, don't do anything
        if(myTransaction.getEffectiveState()!= Txn.State.COMMITTED) return false;

        Txn parentTxn = myTransaction.getParentTransaction();
        //if I have a parent, and he was rolled back, don't do anything
        if(parentTxn.getEffectiveState()== Txn.State.ROLLEDBACK) return false;
        try{
            return visibilityMap.get(txnId,new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    Txn txn = transactionStore.getTransaction(txnId);
                    return isVisible(txn);
                }
            });
        }catch(ExecutionException ee){
            throw new IOException(ee.getCause());
        }

    }

		public Txn getTransaction() {
				return myTransaction;
		}

    @Override
    public int compareTo(DDLFilter o) {
        if (o == null) {
            return 1;
        }
        if (myTransaction.getState()== Txn.State.COMMITTED) {
            if (o.getTransaction().getState() == Txn.State.COMMITTED) {
                return compare(myTransaction.getCommitTimestamp(), o.getTransaction().getCommitTimestamp());
            } else {
                return 1;
            }
        } else {
            if (o.getTransaction().getState()== Txn.State.COMMITTED) {
                return -1;
            } else {
                return compare(myTransaction.getEffectiveBeginTimestamp(), o.getTransaction().getEffectiveBeginTimestamp());
            }
        }
    }

    private static int compare(long my, long other) {
        if (my > other) {
            return 1;
        } else if (my < other) {
            return -1;
        } else {
            return 0;
        }
    }
}
