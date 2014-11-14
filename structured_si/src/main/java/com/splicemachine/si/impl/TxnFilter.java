package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

public interface TxnFilter {
    Filter.ReturnCode filterKeyValue(KeyValue keyValue) throws IOException;
    void nextRow();
    KeyValue produceAccumulatedKeyValue();
    boolean getExcludeRow();

		KeyValueType getType(KeyValue keyValue) throws IOException;
}