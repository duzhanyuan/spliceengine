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

package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.OlapResult;

/**
 * @author Scott Fines
 *         Date: 4/4/16
 */
public class SubmittedResult implements OlapResult{
    private static final long serialVersionUID = 1l;
    private long tickTime;

    public SubmittedResult(){
    }

    public SubmittedResult(long tickTime){
        this.tickTime=tickTime;
    }

    public long getTickTime(){
        return tickTime;
    }

    @Override public boolean isSuccess(){ return false; }

    @Override
    public Throwable getThrowable(){
        return null;
    }
}
