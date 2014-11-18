package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 6/24/14
 */
public class ForwardingLifecycleManager implements TxnLifecycleManager{
		private final TxnLifecycleManager lifecycleManager;

		public ForwardingLifecycleManager(TxnLifecycleManager lifecycleManager) {
				this.lifecycleManager = lifecycleManager;
		}

		@Override
		public Txn beginTransaction() throws IOException {
				Txn txn = lifecycleManager.beginTransaction();
				afterStart(txn);
				return txn;
		}

		@Override
		public Txn beginTransaction(byte[] destinationTable) throws IOException {
				Txn txn = lifecycleManager.beginTransaction(destinationTable);
				afterStart(txn);
				return txn;
		}

		@Override
		public Txn beginTransaction(Txn.IsolationLevel isolationLevel) throws IOException {
				Txn txn = lifecycleManager.beginTransaction(isolationLevel);
				afterStart(txn);
				return txn;
		}

		@Override
		public Txn beginTransaction(Txn.IsolationLevel isolationLevel, byte[] destinationTable) throws IOException {
				Txn txn = lifecycleManager.beginTransaction(isolationLevel, destinationTable);
				afterStart(txn);
				return txn;
		}

		@Override
		public Txn beginChildTransaction(TxnView parentTxn, byte[] destinationTable) throws IOException {
				Txn txn = lifecycleManager.beginChildTransaction(parentTxn, destinationTable);
				afterStart(txn);
				return txn;
		}

		@Override
		public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, byte[] destinationTable) throws IOException {
				Txn txn = lifecycleManager.beginChildTransaction(parentTxn, isolationLevel, destinationTable);
				afterStart(txn);
				return txn;
		}

		@Override
		public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, boolean additive, byte[] destinationTable) throws IOException {
				Txn txn = lifecycleManager.beginChildTransaction(parentTxn, isolationLevel, additive, destinationTable);
				afterStart(txn);
				return txn;
		}

		@Override
		public Txn chainTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, boolean additive, byte[] destinationTable, Txn txnToCommit) throws IOException {
				Txn txn = lifecycleManager.chainTransaction(parentTxn,isolationLevel, additive,destinationTable,txnToCommit);
				afterStart(txn);
				return txn;
		}

		@Override
		public Txn elevateTransaction(Txn txn, byte[] destinationTable) throws IOException {
				Txn txn1 = lifecycleManager.elevateTransaction(txn, destinationTable);
				afterStart(txn1);
				return txn1;
		}

		@Override
		public long commit(long txnId) throws IOException {
				return lifecycleManager.commit(txnId);
		}

		@Override
		public void rollback(long txnId) throws IOException {
				lifecycleManager.rollback(txnId);
		}


		protected void afterStart(Txn txn){
				//no-op by default
		}

}