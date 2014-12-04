/**
 * Copyright 2011 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.sqoop.mapreduce.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * The CouchbaseInputSplit contains a set of VBuckets that an individual tap stream should connect to. There is one
 * input split per mapper.
 */
public class CouchbaseInputSplit extends InputSplit implements Writable {

    // A location here is synonymous with a vbucket.
    private short[] locations;
    private JobContext job;

    public CouchbaseInputSplit() {
        // Empty
    }

    public CouchbaseInputSplit(final short[] curSplit, final JobContext job) {
        this.locations = curSplit;
        this.setJob(job);
        // Empty
    }

    public CouchbaseInputSplit(final short[] locs) {
        locations = locs;
    }

    public CouchbaseInputSplit(final List<Short> locs) {
        locations = new short[locs.size()];

        for (int i = 0; i < locs.size(); i++) {
            locations[i] = locs.get(i);
        }
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return locations.length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        final String[] sLocs = new String[locations.length];

        for (int i = 0; i < locations.length; i++) {
            sLocs[i] = Short.toString(locations[i]);
        }
        return sLocs;
    }

    public short[] getVBuckets() {
        return locations;
    }

    @Override
    public void readFields(final DataInput input) throws IOException {

        final int length = input.readShort();
        locations = new short[length];
        for (int i = 0; i < locations.length; i++) {
            locations[i] = input.readShort();
        }
    }

    @Override
    public void write(final DataOutput output) throws IOException {
        output.writeShort(locations.length);
        for (final short location : locations) {
            output.writeShort(location);
        }
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        for (final short location : locations) {
            builder.append(location);
            builder.append(" ");
        }
        return builder.toString();
    }

    public JobContext getJob() {
        return job;
    }

    public void setJob(final JobContext job) {
        this.job = job;
    }
}
