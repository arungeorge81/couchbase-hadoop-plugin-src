/**
 * Copyright 2011-2012 Couchbase, Inc.
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

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.sqoop.mapreduce.db.DBConfiguration;

import com.cloudera.sqoop.config.ConfigurationHelper;
import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.sqoop.mapreduce.CouchbaseImportMapper;

/**
 * A InputFormat that reads input data from Couchbase Server
 * <p>
 * CouchbaseInputFormat emits LongWritables containing a record number as key and DBWritables as value.
 */
public class CouchbaseInputFormat<T extends DBWritable> extends InputFormat<Text, T> implements Configurable {

    private String tableName;
    private static final Text PASSWORD_SECRET_KEY = new Text(DBConfiguration.PASSWORD_PROPERTY);
    private static CouchbaseConnectionFactory CONN_FACTORY;
    private static CouchbaseConnectionFactoryBuilder CONN_BUILDER_FACTORY;
    private CouchbaseConfiguration dbConf;

    @Override
    public void setConf(final Configuration conf) {
        dbConf = new CouchbaseConfiguration(conf);
        dbConf.setMapperClass(CouchbaseImportMapper.class);
        tableName = dbConf.getInputTableName();
    }

    @Override
    public Configuration getConf() {
        return dbConf.getConf();
    }

    public CouchbaseConfiguration getDBConf() {
        return dbConf;
    }

    @Override
    /** {@inheritDoc} */
    public RecordReader<Text, T> createRecordReader(final InputSplit split, final TaskAttemptContext context)
            throws IOException, InterruptedException {
        System.out.printf("Creating Record Reader %s", split.getLocations().toString());
        return createRecordReader(split, context.getConfiguration());
    }

    @SuppressWarnings("unchecked")
    public RecordReader<Text, T> createRecordReader(final InputSplit split, final Configuration conf)
            throws IOException, InterruptedException {
        final Class<T> inputClass = (Class<T>) dbConf.getInputClass();
        return new CouchbaseRecordReader<T>(inputClass, (CouchbaseInputSplit) split, conf, getDBConf(), tableName);
    }

    @Override
    public List<InputSplit> getSplits(final JobContext job) throws IOException, InterruptedException {
        final List<URI> baseUris = new LinkedList<URI>();
        baseUris.add(URI.create(dbConf.getUrlProperty()));
        // Tell things using Spy's logging to use log4j compat, hadoop uses log4j
        final Properties spyLogProperties = System.getProperties();
        spyLogProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
        System.setProperties(spyLogProperties);
        final String passwordSecurely = new String(job.getCredentials().getSecretKey(
                CouchbaseInputFormat.PASSWORD_SECRET_KEY), Charset.forName("UTF-8"));
        dbConf.setPassword(passwordSecurely);
        System.out.printf("User name is %s and Password from secure store is %s", dbConf.getUsername(),
                passwordSecurely);
        // final CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        // cfb.setOpTimeout(10000); // wait up to 10 seconds for an op to succeed
        // cfb.setOpQueueMaxBlockTime(60000); // wait up to 1 minute to enqueue
        //
        // final CouchbaseClient client = new CouchbaseClient(cfb.buildCouchbaseConnection(baseUris,
        // dbConf.getUsername(),
        // passwordSecurely));

        final AuthDescriptor authDescriptor = new AuthDescriptor(new String[] { "PLAIN" }, new PlainCallbackHandler(
                dbConf.getUsername(), passwordSecurely));
        CouchbaseInputFormat.CONN_BUILDER_FACTORY = new CouchbaseConnectionFactoryBuilder();
        CouchbaseInputFormat.CONN_BUILDER_FACTORY.setOpTimeout(20000);
        CouchbaseInputFormat.CONN_BUILDER_FACTORY.setShouldOptimize(true);
        CouchbaseInputFormat.CONN_BUILDER_FACTORY.setAuthDescriptor(authDescriptor);
        CouchbaseInputFormat.CONN_BUILDER_FACTORY.setProtocol(ConnectionFactoryBuilder.Protocol.BINARY);
        CouchbaseInputFormat.CONN_BUILDER_FACTORY.setMaxReconnectDelay(10000);
        CouchbaseInputFormat.CONN_BUILDER_FACTORY.setOpQueueMaxBlockTime(10000);
        CouchbaseInputFormat.CONN_FACTORY = CouchbaseInputFormat.CONN_BUILDER_FACTORY.buildCouchbaseConnection(
                baseUris, dbConf.getUsername(), "", passwordSecurely);

        final CouchbaseClient client = new CouchbaseClient(CouchbaseInputFormat.CONN_FACTORY);

        final int numVBuckets = client.getNumVBuckets();

        System.out.printf("No of v Buckets is %d", numVBuckets);
        client.shutdown();

        final int chunks = ConfigurationHelper.getJobNumMaps(job);
        final int itemsPerChunk = numVBuckets / chunks;
        int extraItems = numVBuckets % chunks;

        final List<InputSplit> splits = new ArrayList<InputSplit>();

        int splitIndex = 0;
        short[] curSplit = nextEmptySplit(itemsPerChunk, extraItems);
        extraItems--;

        for (short i = 0; i < numVBuckets + 1; i++) {
            if (splitIndex == curSplit.length) {
                final CouchbaseInputSplit split = new CouchbaseInputSplit(curSplit, job);
                splits.add(split);
                curSplit = nextEmptySplit(itemsPerChunk, extraItems);
                extraItems--;
                splitIndex = 0;
            }
            curSplit[splitIndex] = i;
            splitIndex++;
        }
        return splits;
    }

    private short[] nextEmptySplit(final int itemsPerChunk, final int extraItems) {
        if (extraItems > 0) {
            return new short[itemsPerChunk + 1];
        } else {
            return new short[itemsPerChunk];
        }
    }
}
