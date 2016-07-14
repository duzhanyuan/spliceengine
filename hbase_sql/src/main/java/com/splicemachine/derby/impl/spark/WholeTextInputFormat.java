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

package com.splicemachine.derby.impl.spark;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import java.io.IOException;
import java.io.InputStream;

public class WholeTextInputFormat extends CombineFileInputFormat<String, InputStream> implements Configurable {

    private Configuration conf;

    public WholeTextInputFormat() {
    }

    @Override
    public RecordReader<String, InputStream> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        taskAttemptContext.setStatus(inputSplit.toString());
        return new StringInputStreamRecordReader((CombineFileSplit) inputSplit, taskAttemptContext);
    }

    @Override
    public void setConf(Configuration configuration) {
        conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    private class StringInputStreamRecordReader extends RecordReader<String, InputStream> {
        public boolean processed;
        public String key;
        public InputStream value;
        public FileSystem fs;
        public Path path;
        public CombineFileSplit split;

        public StringInputStreamRecordReader(CombineFileSplit inputSplit, TaskAttemptContext taskAttemptContext) {
            this.split = inputSplit;
            this.path = split.getPath(0);
            try {
                this.fs = path.getFileSystem(taskAttemptContext.getConfiguration());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (processed) {
                return false;
            }
            processed = true;

            CompressionCodecFactory factory = new CompressionCodecFactory(conf);
            CompressionCodec codec = factory.getCodec(path);
            key = path.toString();
            FSDataInputStream fileIn = fs.open(path);
            if (codec != null) {
                value = codec.createInputStream(fileIn);
            } else {
                value = fileIn;
            }
            return true;
        }

        @Override
        public String getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public InputStream getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return processed ? 100 : 0;
        }

        @Override
        public void close() throws IOException {

        }
    }
}