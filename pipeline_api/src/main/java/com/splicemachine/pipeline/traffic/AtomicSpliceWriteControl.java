/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.pipeline.traffic;

import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicReference;

/**
 * WriteControl limits (or controls) the rate of writes per region server.  It restricts writes based on the number of writes that are currently "in flight"
 * and the number of writer threads that are currently in use.  WriteControl is essentially a multi-variable counting semaphore where the counting variables
 * are the number of current writes and the number of current writer threads.  The limiting variables (or buckets) or further subdivided into independent and 
 * dependent writes.  Independent writes being writes to a single table and dependent writes being writes that require multiple tables to written to such as
 * a base table and its indexes.  WriteControl does not actually perform writes.  It just controls whether or not the write is allowed to proceed.
 * It essentially gives out "permits" when the write request fits within the control limits and rejects write requests when they don't.
 */

public class AtomicSpliceWriteControl implements SpliceWriteControl{
    private static final Logger LOG = Logger.getLogger(AtomicSpliceWriteControl.class);

    private final AtomicReference<WriteStatus> writeStatus = new AtomicReference<>(new WriteStatus(0, 0, 0, 0));
    protected int maxDependentWriteThreads;
    protected int maxIndependentWriteThreads;
    protected int maxDependentWriteCount;
    protected int maxIndependentWriteCount;

    public AtomicSpliceWriteControl(int maxDependentWriteThreads,
                                    int maxIndependentWriteThreads,int maxDependentWriteCount,int maxIndependentWriteCount) {
        assert (maxDependentWriteThreads >= 0 &&
                maxIndependentWriteThreads >= 0 &&
                maxDependentWriteCount >= 0 &&
                maxIndependentWriteCount >= 0);
        this.maxIndependentWriteThreads = maxIndependentWriteThreads;
        this.maxDependentWriteThreads = maxDependentWriteThreads;
        this.maxDependentWriteCount = maxDependentWriteCount;
        this.maxIndependentWriteCount = maxIndependentWriteCount;
    }

    @Override
    public Status performDependentWrite(int writes) {
        while (true) {
			WriteStatus state = writeStatus.get();
			if (state.dependentWriteThreads > maxDependentWriteThreads
                    || state.dependentWriteCount > maxDependentWriteCount) {
					if (LOG.isTraceEnabled()) {
						LOG.trace(String.format(
								"Rejected dependent write: dependentWriteThreads=%d, maxDependentWriteThreads=%d, dependentWriteCount=%d, maxDependentWriteCount=%d, requestedWriteCount=%d",
								state.dependentWriteThreads, maxDependentWriteThreads, state.dependentWriteCount, maxDependentWriteCount, writes));
					}
					return Status.REJECTED;
			}
			if (writeStatus.compareAndSet(state, WriteStatus.incrementDependentWriteStatus(state,writes))) {
					return Status.DEPENDENT;
			}
        }
    }

    @Override
    public boolean finishDependentWrite(int writes) {
        while (true) {
            WriteStatus state = writeStatus.get();
            if (writeStatus.compareAndSet(state, WriteStatus.decrementDependentWriteStatus(state, writes)))
                return true;
        }
    }

    @Override
    public Status performIndependentWrite(int writes) {
        while (true) {
			WriteStatus state = writeStatus.get();
			if (state.independentWriteThreads > maxIndependentWriteThreads || state.independentWriteCount > maxIndependentWriteCount) {
					if (LOG.isTraceEnabled()) {
						LOG.trace(String.format(
								"Rejected independent write, now attempting to steal from dependent: independentWriteThreads=%d, maxIndependentWriteThreads=%d, independentWriteCount=%d, maxIndependentWriteCount=%d, requestedWriteCount=%d",
								state.independentWriteThreads, maxIndependentWriteThreads, state.independentWriteCount, maxIndependentWriteCount, writes));
					}
					return (performDependentWrite(writes)); // Attempt to steal
			}
			if (writeStatus.compareAndSet(state, WriteStatus.incrementIndependentWriteStatus(state,writes))) {
					return Status.INDEPENDENT;
			}
        }
    }

    @Override
    public boolean finishIndependentWrite(int writes) {
        while (true) {
            WriteStatus state = writeStatus.get();
            if (writeStatus.compareAndSet(state, WriteStatus.decrementIndependentWriteStatus(state, writes)))
                return true;
        }
    }

    @Override
    public WriteStatus getWriteStatus() {
        return writeStatus.get();
    }

    @Override
    public int maxDependendentWriteThreads(){
        return maxDependentWriteThreads;
    }

    @Override
    public int maxIndependentWriteThreads(){
        return maxIndependentWriteThreads;
    }

    @Override
    public int maxDependentWriteCount(){
        return maxDependentWriteCount;
    }

    @Override
    public int maxIndependentWriteCount(){
        return maxIndependentWriteCount;
    }

    @Override
    public void setMaxIndependentWriteThreads(int newMaxIndependentWriteThreads){
        this.maxIndependentWriteThreads = newMaxIndependentWriteThreads;
    }

    @Override
    public void setMaxDependentWriteThreads(int newMaxDependentWriteThreads){
        this.maxDependentWriteThreads = newMaxDependentWriteThreads;
    }

    @Override
    public void setMaxIndependentWriteCount(int newMaxIndependentWriteCount){
        this.maxIndependentWriteCount = newMaxIndependentWriteCount;
    }

    @Override
    public void setMaxDependentWriteCount(int newMaxDependentWriteCount){
        this.maxDependentWriteCount = newMaxDependentWriteCount;
    }

}
