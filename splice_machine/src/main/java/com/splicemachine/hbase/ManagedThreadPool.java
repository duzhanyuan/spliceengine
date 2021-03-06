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

package com.splicemachine.hbase;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Created on: 3/19/13
 */
public class ManagedThreadPool implements ExecutorService,JMXThreadPool {
    private final ThreadPoolExecutor pool;
    private final AtomicInteger totalRejected = new AtomicInteger(0);

    public ManagedThreadPool(ThreadPoolExecutor pool) {
        this.pool = pool;
        pool.setRejectedExecutionHandler(
                new CountingRejectionHandler(pool.getRejectedExecutionHandler()));
    }

    @Override
    public void shutdown() {
        this.pool.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return pool.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return pool.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return pool.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return pool.awaitTermination(timeout,unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return pool.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return pool.submit(task,result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return pool.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return pool.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return pool.invokeAll(tasks,timeout,unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return pool.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return pool.invokeAny(tasks,timeout,unit);
    }

    @Override
    public void execute(Runnable command) {
        pool.execute(command);
    }

    @Override
    public int getCorePoolSize(){
        return pool.getCorePoolSize();
    }

    @Override
    public void setCorePoolSize(int corePoolSize) {
        pool.setCorePoolSize(corePoolSize);
    }

    @Override
    public int getMaximumPoolSize(){
        return pool.getMaximumPoolSize();
    }

    @Override
    public void setMaximumPoolSize(int maximumPoolSize) {
        pool.setMaximumPoolSize(maximumPoolSize);
    }

    @Override
    public long getThreadKeepAliveTime(){
        return pool.getKeepAliveTime(TimeUnit.MILLISECONDS);
    }
    @Override
    public void setThreadKeepAliveTime(long timeMs) {
        pool.setKeepAliveTime(timeMs,TimeUnit.MILLISECONDS);
    }

    @Override
    public int getCurrentPoolSize() {
        return pool.getPoolSize();
    }

    @Override
    public int getCurrentlyExecutingThreads() {
        return pool.getActiveCount();
    }

    @Override
    public int getLargestPoolSize() {
        return pool.getLargestPoolSize();
    }

    @Override
    public long getTotalScheduledTasks() {
        return pool.getTaskCount();
    }

    @Override
    public long getTotalCompletedTasks() {
        return pool.getCompletedTaskCount();
    }

    @Override
    public int getCurrentlyAvailableThreads(){
        int maxThreads = pool.getMaximumPoolSize();
        if(maxThreads ==Integer.MAX_VALUE) return Integer.MAX_VALUE;
        return maxThreads-pool.getActiveCount();
    }

    @Override
    public int getPendingTasks(){
        return 0; //TODO -sf- implement
    }

    @Override
    public long getTotalRejectedTasks(){
        return totalRejected.get();
    }

    private class CountingRejectionHandler implements RejectedExecutionHandler {
        private final RejectedExecutionHandler delegate;
        public CountingRejectionHandler(RejectedExecutionHandler rejectedExecutionHandler) {
            this.delegate = rejectedExecutionHandler;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            totalRejected.incrementAndGet();
            if(delegate!=null)
                delegate.rejectedExecution(r,executor);
        }
    }
}
