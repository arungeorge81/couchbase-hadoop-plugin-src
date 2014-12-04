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
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.util.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

/**
 * A OutputFormat that sends the reduce output to a Couchbase server.
 * <p>
 * {@link CocuhbaseOutputFormat} accepts &lt;key,value&gt; pairs, where key has a type extending DBWritable. Returned
 * {@link RecordWriter} writes <b>only the key</b> to the database with a batch tap stream.
 */
public class CouchbaseOutputFormat<K extends DBWritable, V> extends OutputFormat<K, V> {
    public static final Log LOG = LogFactory.getLog(CouchbaseOutputFormat.class.getName());

    private static final Text PASSWORD_SECRET_KEY = new Text(DBConfiguration.PASSWORD_PROPERTY);

    private static CouchbaseConnectionFactory CONN_FACTORY;
    private static CouchbaseConnectionFactoryBuilder CONN_BUILDER_FACTORY;

    private JobContext jobContext;

    @Override
    public void checkOutputSpecs(final JobContext context) throws IOException, InterruptedException {
        this.jobContext = context;
        final Configuration conf = context.getConfiguration();

        // Sanity check all the configuration values we need.
        if (null == conf.get(DBConfiguration.URL_PROPERTY)) {
            throw new IOException("Database connection URL is not set.");
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context); // TODO: see if this can be
                                                                                          // removed. It doesn't hurt.
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(final TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new CouchbaseRecordWriter(new CouchbaseConfiguration(context.getConfiguration()));
    }

    /**
     * A RecordWriter that writes the reduce output to a Couchbase server.
     */
    public class CouchbaseRecordWriter extends RecordWriter<K, V> {

        final class KV {
            private final String key;
            private final Object value;
            private final Future<Boolean> status;

            private KV(final String key, final Object value, final Future<Boolean> status) {
                this.key = key;
                this.value = value;
                this.status = status;
            }
        }

        private CouchbaseClient client;

        private final BlockingQueue<KV> opQ;

        public CouchbaseRecordWriter(final CouchbaseConfiguration dbConf) {
            dbConf.getUsername();
            final String passwordSecurely = new String(jobContext.getCredentials().getSecretKey(
                    CouchbaseOutputFormat.PASSWORD_SECRET_KEY));
            System.out.printf("CouchbaseRecordWriter: Found password %s", passwordSecurely);

            final String url = dbConf.getUrlProperty();
            opQ = new LinkedBlockingQueue<KV>();
            // final CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
            // cfb.setOpTimeout(10000); // wait up to 10 seconds for an op to succeed
            // cfb.setOpQueueMaxBlockTime(60000); // wait up to 1 minute to enqueue
            // try {
            // final List<URI> baselist = Arrays.asList(new URI(url));
            // // Tell things using Spy's logging to use log4j compat, hadoop uses log4j
            // final Properties spyLogProperties = System.getProperties();
            // spyLogProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
            // System.setProperties(spyLogProperties);
            // client = new CouchbaseClient(cfb.buildCouchbaseConnection(baselist, user, user, pass));
            // } catch (final IOException e) {
            // client.shutdown();
            // CouchbaseOutputFormat.LOG.fatal("Problem configuring CouchbaseClient for IO.", e);
            // } catch (final URISyntaxException e) {
            // client.shutdown();
            // CouchbaseOutputFormat.LOG.fatal("Could not configure CouchbaseClient with URL supplied: " + url, e);
            // }

            // Tell things using Spy's logging to use log4j compat, hadoop uses log4j
            final Properties spyLogProperties = System.getProperties();
            spyLogProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
            System.setProperties(spyLogProperties);

            final AuthDescriptor authDescriptor = new AuthDescriptor(new String[] { "PLAIN" },
                    new PlainCallbackHandler(dbConf.getUsername(), passwordSecurely));
            CouchbaseOutputFormat.CONN_BUILDER_FACTORY = new CouchbaseConnectionFactoryBuilder();
            CouchbaseOutputFormat.CONN_BUILDER_FACTORY.setOpTimeout(20000);
            CouchbaseOutputFormat.CONN_BUILDER_FACTORY.setShouldOptimize(true);
            CouchbaseOutputFormat.CONN_BUILDER_FACTORY.setAuthDescriptor(authDescriptor);
            CouchbaseOutputFormat.CONN_BUILDER_FACTORY.setProtocol(ConnectionFactoryBuilder.Protocol.BINARY);
            CouchbaseOutputFormat.CONN_BUILDER_FACTORY.setMaxReconnectDelay(10000);
            CouchbaseOutputFormat.CONN_BUILDER_FACTORY.setOpQueueMaxBlockTime(10000);
            try {
                CouchbaseOutputFormat.CONN_FACTORY = CouchbaseOutputFormat.CONN_BUILDER_FACTORY
                        .buildCouchbaseConnection(Arrays.asList(new URI(url)), dbConf.getUsername(), "",
                                passwordSecurely);
                client = new CouchbaseClient(CouchbaseOutputFormat.CONN_FACTORY);
            } catch (final IOException e) {
                client.shutdown();
                CouchbaseOutputFormat.LOG.fatal("Could not configure CouchbaseClient with URL supplied: " + url, e);

            } catch (final URISyntaxException e) {
                client.shutdown();
                CouchbaseOutputFormat.LOG.fatal("Could not configure CouchbaseClient with URL supplied: " + url, e);

            }

        }

        @Override
        public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
            while (opQ.size() > 0) {
                drainQ();
            }
            client.shutdown();
        }

        private void drainQ() throws IOException {
            final Queue<KV> list = new LinkedList<KV>();
            opQ.drainTo(list, opQ.size());

            KV kv;
            while ((kv = list.poll()) != null) {
                try {
                    if (!kv.status.get().booleanValue()) {
                        opQ.add(new KV(kv.key, kv.value, client.set(kv.key, 0, kv.value)));
                        CouchbaseOutputFormat.LOG.info("Failed");
                    }
                } catch (final RuntimeException e) {
                    CouchbaseOutputFormat.LOG.fatal("Failed to export record. " + e.getMessage());
                    throw new IOException("Failed to export record", e);
                } catch (final InterruptedException e) {
                    CouchbaseOutputFormat.LOG.fatal("Interrupted during record export. " + e.getMessage());
                    throw new IOException("Interrupted during record export", e);
                } catch (final ExecutionException e) {
                    CouchbaseOutputFormat.LOG.fatal("Aborted execution during record export. " + e.getMessage());
                    throw new IOException("Aborted execution during record export", e);
                }
            }
        }

        @Override
        public void write(final K key, final V value) throws IOException, InterruptedException {
            String keyToAdd = null;
            Object valueToSet = null;

            if (opQ.size() > 10000) {
                drainQ(); // TODO: probably don't need this with new queue tuning
            }

            final SqoopRecord recordToExport = (SqoopRecord) key;
            final Map<String, Object> recordFields = recordToExport.getFieldMap();

            keyToAdd = recordFields.get("Key").toString();
            valueToSet = recordFields.get("Value");
            OperationFuture<Boolean> arecord = null;

            try {
                StringUtils.validateKey(keyToAdd);
            } catch (final IllegalArgumentException ex) {
                // warn and try to transform the key
                String replacement = keyToAdd.replace(" ", "_");
                replacement = replacement.replace("\n", "");
                replacement = replacement.replace("\r", "");
                replacement = replacement.replace("\t", "-");
                CouchbaseOutputFormat.LOG.warn("Key to record supplied invalid.  Will attempt to transform."
                                               + " Replacing: \"" + keyToAdd + "\" with \"" + replacement + "\"");
                keyToAdd = replacement;
            }

            try {
                if (valueToSet == null) {
                    CouchbaseOutputFormat.LOG.warn("Value to be stored for key \"" + keyToAdd + "\" "
                                                   + " is null, storing empty string as a replacement.");
                    arecord = client.set(keyToAdd, 0, "");
                } else {
                    arecord = client.set(keyToAdd, 0, valueToSet);
                }
                opQ.add(new KV(keyToAdd, valueToSet, arecord));
            } catch (final IllegalArgumentException e) {
                CouchbaseOutputFormat.LOG.error("Failed to write record with key \"" + key.toString()
                                                + "\" with class type canonical " + value.getClass().getCanonicalName()
                                                + " and simple name " + value.getClass().getSimpleName());
                if (arecord != null) {
                    CouchbaseOutputFormat.LOG.error("Failed to write record: \"" + arecord.getKey() + "\"", e);
                    CouchbaseOutputFormat.LOG.error("Status of failed record is " + arecord.getStatus());
                }
                throw new IOException("Failed to write record", e);
            }
        }
    }
}
