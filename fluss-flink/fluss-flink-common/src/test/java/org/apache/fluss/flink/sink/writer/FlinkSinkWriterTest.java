/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.sink.writer;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.fluss.flink.sink.writer.FlinkSinkWriter}. */
public class FlinkSinkWriterTest extends FlinkTestBase {
    private static final String DEFAULT_SINK_DB = "test-sink-db";

    private static final TablePath DEFAULT_SINK_TABLE_PATH =
            TablePath.of(DEFAULT_SINK_DB, "test-sink-table");

    private static final TableDescriptor TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("id", org.apache.fluss.types.DataTypes.INT())
                                    .column("name", org.apache.fluss.types.DataTypes.CHAR(10))
                                    .build())
                    .build();

    @ParameterizedTest
    @ValueSource(strings = {"", "1"})
    void testSinkMetrics(String clientId) throws Exception {
        admin.createDatabase(
                DEFAULT_SINK_TABLE_PATH.getDatabaseName(), DatabaseDescriptor.EMPTY, true);
        createTable(DEFAULT_SINK_TABLE_PATH, TABLE_DESCRIPTOR);

        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        flussConf.set(ConfigOptions.CLIENT_ID, clientId);

        InterceptingOperatorMetricGroup interceptingOperatorMetricGroup =
                new InterceptingOperatorMetricGroup();
        MockWriterInitContext mockWriterInitContext =
                new MockWriterInitContext(interceptingOperatorMetricGroup);
        FlinkSinkWriter<RowData> flinkSinkWriter =
                createSinkWriter(flussConf, mockWriterInitContext.getMailboxExecutor());

        flinkSinkWriter.initialize(mockWriterInitContext.metricGroup());
        flinkSinkWriter.write(
                GenericRowData.of(1, StringData.fromString("a")), new MockSinkWriterContext());
        flinkSinkWriter.flush(false);

        Metric currentSendTime = interceptingOperatorMetricGroup.get(MetricNames.CURRENT_SEND_TIME);
        assertThat(currentSendTime).isInstanceOf(Gauge.class);
        // the default send latency is -1, so check it is >= 0, as the latency maybe very small 0ms
        assertThat(((Gauge<Long>) currentSendTime).getValue()).isGreaterThanOrEqualTo(0);

        Metric numRecordSend = interceptingOperatorMetricGroup.get(MetricNames.NUM_RECORDS_SEND);
        assertThat(numRecordSend).isInstanceOf(Counter.class);
        assertThat(((Counter) numRecordSend).getCount()).isGreaterThan(0);

        flinkSinkWriter.close();
    }

    @Test
    void testFlussSchemaChange() throws Exception {
        admin.createDatabase(
                DEFAULT_SINK_TABLE_PATH.getDatabaseName(), DatabaseDescriptor.EMPTY, true);
        createTable(DEFAULT_SINK_TABLE_PATH, TABLE_DESCRIPTOR);
        admin.alterTable(
                        DEFAULT_SINK_TABLE_PATH,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "c1",
                                        DataTypes.STRING(),
                                        null,
                                        TableChange.ColumnPosition.last())),
                        false)
                .get();

        Configuration clientConfig = FLUSS_CLUSTER_EXTENSION.getClientConfig();

        MockWriterInitContext mockWriterInitContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());
        try (FlinkSinkWriter<RowData> writer =
                createSinkWriter(clientConfig, mockWriterInitContext.getMailboxExecutor())) {
            writer.initialize(mockWriterInitContext.metricGroup());
            // case1: write data which is matched flink schema.
            writer.write(
                    GenericRowData.of(1, StringData.fromString("a")), new MockSinkWriterContext());
            writer.flush(false);

            // case2: write data which lacks the last column of flink schema
            writer.write(GenericRowData.of(1), new MockSinkWriterContext());
            writer.flush(false);

            // case3: write data which has reorder column of flink schema
            assertThatThrownBy(
                            () ->
                                    writer.write(
                                            GenericRowData.of(StringData.fromString("a"), 1),
                                            new MockSinkWriterContext()))
                    .rootCause()
                    .isExactlyInstanceOf(ClassCastException.class)
                    .hasMessageContaining(
                            "class org.apache.flink.table.data.binary.BinaryStringData cannot be cast to class java.lang.Integer");
            writer.flush(false);
        }
    }

    @Test
    void testWriteExceptionWhenFlussUnavailable() throws Exception {
        testExceptionWhenFlussUnavailable(
                (writer, mailboxExecutor) -> {
                    // Flush client here to make sure last write is finished but not check
                    // exception.
                    writer.getTableWriter().flush();
                    assertThatThrownBy(
                                    () ->
                                            writer.write(
                                                    GenericRowData.of(
                                                            2, StringData.fromString("b")),
                                                    new MockSinkWriterContext()))
                            .hasRootCauseExactlyInstanceOf(NetworkException.class);
                });
    }

    @Test
    void testFlushExceptionWhenFlussUnavailable() throws Exception {
        testExceptionWhenFlussUnavailable(
                (writer, mailboxExecutor) -> {
                    // Flush SinkWriter here to make sure last write finish and also check
                    // exception.
                    assertThatThrownBy(() -> writer.flush(true))
                            .hasRootCauseExactlyInstanceOf(NetworkException.class);
                });
    }

    @Test
    void testCloseExceptionWhenFlussUnavailable() throws Exception {
        testExceptionWhenFlussUnavailable(
                (writer, mailboxExecutor) -> {
                    // Flush client here to make sure last write is finished but not check
                    // exception.
                    writer.getTableWriter().flush();
                    assertThatThrownBy(writer::close)
                            .hasRootCauseExactlyInstanceOf(NetworkException.class);
                });
    }

    @Test
    void testMailBoxExceptionWhenFlussUnavailable() throws Exception {
        testExceptionWhenFlussUnavailable(
                (writer, mailboxExecutor) -> {
                    // Flush client here to make sure last write is finished but not check
                    // exception.
                    writer.getTableWriter().flush();
                    assertThatThrownBy(
                                    () -> {
                                        while (mailboxExecutor.tryYield()) {
                                            // execute all mails
                                        }
                                    })
                            .hasRootCauseExactlyInstanceOf(NetworkException.class);
                });
    }

    private void testExceptionWhenFlussUnavailable(
            BiConsumer<FlinkSinkWriter<RowData>, MailboxExecutor> actionAfterFlussUnavailable)
            throws Exception {
        FlussClusterExtension flussClusterExtension = FlussClusterExtension.builder().build();
        try {
            flussClusterExtension.start();

            // prepare table
            Configuration clientConfig = flussClusterExtension.getClientConfig();
            clientConfig.set(ConfigOptions.CLIENT_WRITER_RETRIES, 0);
            clientConfig.set(ConfigOptions.CLIENT_WRITER_ENABLE_IDEMPOTENCE, false);
            try (Connection connection = ConnectionFactory.createConnection(clientConfig);
                    Admin admin = connection.getAdmin()) {
                admin.createDatabase(
                                DEFAULT_SINK_TABLE_PATH.getDatabaseName(),
                                DatabaseDescriptor.EMPTY,
                                true)
                        .get();
                admin.createTable(DEFAULT_SINK_TABLE_PATH, TABLE_DESCRIPTOR, true).get();
            }

            MockWriterInitContext mockWriterInitContext =
                    new MockWriterInitContext(new InterceptingOperatorMetricGroup());
            // test fluss unavailable.
            try (FlinkSinkWriter<RowData> writer =
                    createSinkWriter(clientConfig, mockWriterInitContext.getMailboxExecutor())) {
                writer.initialize(mockWriterInitContext.metricGroup());
                flussClusterExtension.close();
                writer.write(
                        GenericRowData.of(1, StringData.fromString("a")),
                        new MockSinkWriterContext());
                actionAfterFlussUnavailable.accept(
                        writer, mockWriterInitContext.getMailboxExecutor());
            }
        } finally {
            flussClusterExtension.close();
        }
    }

    private FlinkSinkWriter<RowData> createSinkWriter(
            Configuration configuration, MailboxExecutor mailboxExecutor) throws Exception {
        RowType tableRowType =
                RowType.of(
                        new LogicalType[] {new IntType(), new CharType(10)},
                        new String[] {"id", "name"});
        RowDataSerializationSchema serializationSchema =
                new RowDataSerializationSchema(true, false);
        return new AppendSinkWriter<>(
                DEFAULT_SINK_TABLE_PATH,
                configuration,
                tableRowType,
                mailboxExecutor,
                serializationSchema);
    }

    @Test
    void testTableInfoAutoUpdate() throws Exception {
        String testDb = "test-auto-update-db";
        TablePath testTablePath = TablePath.of(testDb, "test-auto-update-table");

        // Create database
        admin.createDatabase(testDb, DatabaseDescriptor.EMPTY, true).get();

        // Create log table with 3 buckets (no primary key)
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .build())
                        .distributedBy(3)
                        .build();
        createTable(testTablePath, tableDescriptor);

        Configuration clientConfig = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        MockWriterInitContext mockWriterInitContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Create AppendSinkWriter
        RowType tableRowType =
                RowType.of(
                        new LogicalType[] {new IntType(), new CharType(10)},
                        new String[] {"id", "name"});
        RowDataSerializationSchema serializationSchema =
                new RowDataSerializationSchema(true, false);
        AppendSinkWriter<RowData> writer =
                new AppendSinkWriter<>(
                        testTablePath,
                        clientConfig,
                        tableRowType,
                        mockWriterInitContext.getMailboxExecutor(),
                        serializationSchema);

        try {
            writer.initialize(mockWriterInitContext.metricGroup());

            // Step 1: Write data with 3 buckets, verify success
            for (int i = 0; i < 10; i++) {
                writer.write(
                        GenericRowData.of(i, StringData.fromString("name" + i)),
                        new MockSinkWriterContext());
            }
            writer.flush(false);

            // Verify data is written to 3 buckets
            Map<Integer, Integer> bucketCounts = countRecordsPerBucket(testTablePath, 3);
            assertThat(bucketCounts.size()).isEqualTo(3);
            int totalRecords = bucketCounts.values().stream().mapToInt(Integer::intValue).sum();
            assertThat(totalRecords).isEqualTo(10);

            // Step 2: Alter table bucket number to 4
            admin.alterTable(
                            testTablePath,
                            Collections.singletonList(TableChange.set("bucket.num", "4")),
                            false)
                    .get();

            // Wait for schema sync
            FLUSS_CLUSTER_EXTENSION.waitAllSchemaSync(testTablePath, 2);

            // Step 3: Force update table by setting lastRefreshTime to trigger refresh
            Field lastRefreshTimeField = FlinkSinkWriter.class.getDeclaredField("lastRefreshTime");
            lastRefreshTimeField.setAccessible(true);
            lastRefreshTimeField.set(
                    writer, System.currentTimeMillis() - 61000); // Set to 61 seconds ago

            // Step 4: Write more data, should use 4 buckets now
            for (int i = 10; i < 20; i++) {
                writer.write(
                        GenericRowData.of(i, StringData.fromString("name" + i)),
                        new MockSinkWriterContext());
            }
            writer.flush(false);

            // Step 5: Verify data is written to 4 buckets
            Map<Integer, Integer> newBucketCounts = countRecordsPerBucket(testTablePath, 4);
            assertThat(newBucketCounts.size()).isEqualTo(4);
            int newTotalRecords =
                    newBucketCounts.values().stream().mapToInt(Integer::intValue).sum();
            assertThat(newTotalRecords).isEqualTo(20); // Total records from both writes

            // Verify that we have records in all 4 buckets
            Set<Integer> bucketsWithData = newBucketCounts.keySet();
            assertThat(bucketsWithData).hasSize(4);
            for (int bucket = 0; bucket < 4; bucket++) {
                assertThat(bucketsWithData).contains(bucket);
            }
        } finally {
            writer.close();
        }
    }

    private Map<Integer, Integer> countRecordsPerBucket(TablePath tablePath, int expectedBuckets)
            throws Exception {
        Map<Integer, Integer> bucketCounts = new HashMap<>();
        Configuration clientConfig = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        try (Connection connection = ConnectionFactory.createConnection(clientConfig);
                Table table = connection.getTable(tablePath);
                LogScanner logScanner = table.newScan().createLogScanner()) {
            // Subscribe to all buckets from beginning
            for (int bucket = 0; bucket < expectedBuckets; bucket++) {
                logScanner.subscribeFromBeginning(bucket);
            }

            // Collect all records and count by bucket
            int totalScanned = 0;
            int maxRecords = 50; // Limit to avoid infinite loop
            while (totalScanned < maxRecords) {
                org.apache.fluss.client.table.scanner.log.ScanRecords scanRecords =
                        logScanner.poll(Duration.ofSeconds(1));
                if (scanRecords.isEmpty()) {
                    break;
                }
                for (TableBucket tableBucket : scanRecords.buckets()) {
                    int bucketId = tableBucket.getBucket();
                    int recordCount = scanRecords.records(tableBucket).size();
                    bucketCounts.put(
                            bucketId, bucketCounts.getOrDefault(bucketId, 0) + recordCount);
                    totalScanned += recordCount;
                }
            }
        }
        return bucketCounts;
    }

    static class MockSinkWriterContext implements SinkWriter.Context {
        @Override
        public long currentWatermark() {
            return 0;
        }

        @Override
        public Long timestamp() {
            return 0L;
        }
    }
}
