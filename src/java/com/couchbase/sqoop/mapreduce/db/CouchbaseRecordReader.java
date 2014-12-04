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
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.naming.ConfigurationException;

import net.spy.memcached.tapmessage.MessageBuilder;
import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.tapmessage.TapStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.ReflectionUtils;

import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.couchbase.client.TapClient;
import com.couchbase.sqoop.lib.CouchbaseRecordUtil;

/**
 * A RecordReader that reads records from a tap stream Emits LongWritables containing the record number as key and
 * DBWritables as value.
 */
public class CouchbaseRecordReader<T extends DBWritable> extends RecordReader<Text, T> {

    private static final Log LOG = LogFactory.getLog(CouchbaseRecordReader.class);
    private static final Text PASSWORD_SECRET_KEY = new Text(DBConfiguration.PASSWORD_PROPERTY);

    private final Class<T> inputClass;

    private final Configuration conf;

    private final Text key = null;

    private T value = null;

    private final CouchbaseConfiguration dbConf;

    private final String tableName;

    private TapClient client;

    private final CouchbaseInputSplit split;

    public CouchbaseRecordReader(final Class<T> inputClass, final CouchbaseInputSplit split, final Configuration conf,
            final CouchbaseConfiguration dbConfig, final String table) {
        this.inputClass = inputClass;
        this.conf = conf;
        this.dbConf = dbConfig;
        this.tableName = table;
        this.split = split;
        try {
            final String user = dbConf.getUsername();
            final String pass = dbConf.getPassword();
            final String url = dbConf.getUrlProperty();
            // final String passwordSecurely = new String(dbConfig.getConf().get(DBConfiguration.PASSWORD_PROPERTY));
            // System.out.printf("CouchbaseRecorder: Found password %s", passwordSecurely);

            // final String passwordSecurely = new String(split.getJob().getCredentials()
            // .getSecretKey(CouchbaseRecordReader.PASSWORD_SECRET_KEY));
            // pass = passwordSecurely;
            this.client = new TapClient(Arrays.asList(new URI(url)), user, pass);
        } catch (final URISyntaxException e) {
            CouchbaseRecordReader.LOG.error("Bad URI Syntax: " + e.getMessage());
            client.shutdown();
        }
    }

    @Override
    public void close() throws IOException {
        client.shutdown();
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        CouchbaseRecordReader.LOG.trace("Key: " + key);
        return key;
    }

    @Override
    public T getCurrentValue() throws IOException, InterruptedException {
        CouchbaseRecordReader.LOG.trace("Value: " + value.toString());
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        // Since we don't know how many messages are coming progress doesn't
        // make much sense so either we're all the way done or not done at all.
        if (client.hasMoreMessages()) {
            return 0;
        }
        return 1;
    }

    @Override
    public void initialize(final InputSplit splits, final TaskAttemptContext context) throws IOException,
            InterruptedException {
        try {
            // if (this.client == null) {
            // final String user = dbConf.getUsername();
            // String pass = dbConf.getPassword();
            // final String url = dbConf.getUrlProperty();
            // // final String passwordSecurely = new
            // // String(dbConfig.getConf().get(DBConfiguration.PASSWORD_PROPERTY));
            // // System.out.printf("CouchbaseRecorder: Found password %s", passwordSecurely);
            // System.out.printf("CouchbaseRecordReader: %s", splits);
            // final String passwordSecurely = new String(((CouchbaseInputSplit) splits).getJob().getCredentials()
            // .getSecretKey(CouchbaseRecordReader.PASSWORD_SECRET_KEY));
            // pass = passwordSecurely;
            // try {
            // this.client = new TapClient(Arrays.asList(new URI(url)), user, pass);
            // } catch (final URISyntaxException e) {
            // this.client.shutdown();
            // }
            // }

            final MessageBuilder builder = new MessageBuilder();
            if (tableName.equals("DUMP")) {
                builder.doDump();
                builder.supportAck();
                builder.specifyVbuckets(split.getVBuckets());
                client.tapCustom(null, builder.getMessage());
            } else if (tableName.startsWith("BACKFILL_")) {
                final String time = tableName.substring("BACKFILL_".length(), tableName.length());
                builder.doBackfill(0);
                builder.supportAck();
                builder.specifyVbuckets(split.getVBuckets());
                final TapStream tapStream = client.tapCustom(null, builder.getMessage());
                createTapStreamTimeout(tapStream, new Long(time).intValue());
            } else {
                throw new IOException("Table name must be \"DUMP\" or begin with "
                                      + "\"BACKFILL_\" followed by an integer.");
            }
        } catch (final ConfigurationException e) {
            CouchbaseRecordReader.LOG.error("Couldn't Configure Tap Stream: " + e.getMessage());
            client.shutdown();
        } catch (final NumberFormatException e) {
            CouchbaseRecordReader.LOG.error("Bad Backfill Time: " + e.getMessage() + "\n(Ex. BACKFILL_5");
            client.shutdown();
        }
    }

    private void createTapStreamTimeout(final TapStream tapStream, final long duration) {
        if (duration > 0) {
            final Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(TimeUnit.MILLISECONDS.convert(duration, TimeUnit.MINUTES));
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        CouchbaseRecordReader.LOG.error("Tap stream closing early. Reason: " + e.getMessage());
                    }
                    tapStream.cancel();
                }
            };
            new Thread(r).start();
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        ResponseMessage message;
        while ((message = client.getNextMessage()) == null) {
            if (!client.hasMoreMessages()) {
                CouchbaseRecordReader.LOG.info("All TAP messages have been received.\n");
                return false;
            }
        }

        if (value == null) {
            /* Will create a new value based on the generated ORM mapper.
             * This only happens the first time through.
             */
            value = ReflectionUtils.newInstance(inputClass, conf);
        }

        final String recordKey = message.getKey();
        if (recordKey == null) {
            ((SqoopRecord) value).setField("Key", null);
            CouchbaseRecordReader.LOG.error("Received record with no key.  Attempting to continue."
                                            + "  ResponseMessage received:\n" + message);
        } else {
            ((SqoopRecord) value).setField("Key", recordKey);
        }
        ((SqoopRecord) value).setField("Value", CouchbaseRecordUtil.deserialize(message).toString());

        return true;
    }

}
