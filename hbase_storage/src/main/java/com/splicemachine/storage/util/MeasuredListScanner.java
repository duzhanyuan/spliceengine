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

package com.splicemachine.storage.util;

import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.List;


/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class MeasuredListScanner implements AutoCloseable{
    private final RegionScanner delegate;
    private final Timer timer;
    private final Counter filterCounter;
    private final Counter outputBytesCounter;

    public MeasuredListScanner(RegionScanner delegate,MetricFactory metricFactory){
        this.delegate=delegate;
        this.timer = metricFactory.newTimer();
        this.filterCounter = metricFactory.newCounter();
        this.outputBytesCounter = metricFactory.newCounter();
    }

    public boolean next(List<Cell> list) throws IOException{
        timer.startTiming();
        boolean b=delegate.nextRaw(list);
        timer.tick(list.size()>0?1l:0l);

        if(outputBytesCounter.isActive())
            countOutput(list);
        return b;
    }

    public boolean next(List<Cell> list,int limit) throws IOException{
        //TODO -sf- do not ignore the limit
        return next(list);
    }

    public void close() throws IOException{
        delegate.close();
    }

    /*Metrics reporting*/
    public TimeView getReadTime(){ return timer.getTime(); }
    public long getBytesOutput(){ return outputBytesCounter.getTotal(); }
    public long getRowsFiltered(){ return filterCounter.getTotal(); }
    public long getRowsVisited(){ return timer.getNumEvents(); }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void countOutput(List<Cell> list){
        //TODO -sf- count the output
    }
}
