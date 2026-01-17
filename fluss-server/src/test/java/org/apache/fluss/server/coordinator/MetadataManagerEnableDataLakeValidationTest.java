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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.lake.lakestorage.LakeSnapshotProvider;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.utils.json.TableBucketOffsets;

import org.apache.zookeeper.KeeperException;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for strict checks when enabling datalake in {@link MetadataManager}. */
class MetadataManagerEnableDatalakeValidationTest {

    @Test
    void testExtractTableIdFromJsonOffsets() throws Exception {
        TableBucketOffsets offsets =
                new TableBucketOffsets(123L, Collections.emptyMap());
        byte[] jsonBytes = offsets.toJsonBytes();

        MetadataManager manager = createMetadataManager();
        long tableId =
                invokeExtractTableIdFromFlussOffsets(manager, new String(jsonBytes, StandardCharsets.UTF_8));
        assertThat(tableId).isEqualTo(123L);
    }

    @Test
    void testExtractTableIdFromPathOffsets() throws Exception {
        String path =
                "/remote/data/lake/db/test_table-456/metadata/00000000-0000-0000-0000-000000000000.offsets";

        MetadataManager manager = createMetadataManager();
        long tableId = invokeExtractTableIdFromFlussOffsets(manager, path);
        assertThat(tableId).isEqualTo(456L);
    }

    @Test
    void testValidateFlussOffsetsConsistencyThrowsOnMissingOffsets() throws Exception {
        MetadataManager manager = createMetadataManager();
        LakeSnapshotProvider.LakeSnapshotMetadata metadata =
                new LakeSnapshotProvider.LakeSnapshotMetadata(1L, 0L, null);

        assertThatThrownBy(
                        () ->
                                invokeValidateFlussOffsetsConsistencyOnEnable(
                                        manager,
                                        TablePath.of("db", "tbl"),
                                        1L,
                                        metadata))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("does not contain 'fluss-offsets'");
    }

    @Test
    void testValidateFlussOffsetsConsistencyThrowsOnTableIdMismatch() throws Exception {
        TableBucketOffsets offsets =
                new TableBucketOffsets(999L, Collections.emptyMap());
        String json = new String(offsets.toJsonBytes(), StandardCharsets.UTF_8);

        MetadataManager manager = createMetadataManager();
        LakeSnapshotProvider.LakeSnapshotMetadata metadata =
                new LakeSnapshotProvider.LakeSnapshotMetadata(1L, 0L, json);

        assertThatThrownBy(
                        () ->
                                invokeValidateFlussOffsetsConsistencyOnEnable(
                                        manager,
                                        TablePath.of("db", "tbl"),
                                        1L,
                                        metadata))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("belongs to another Fluss table");
    }

    @Test
    void testValidateTtlWindowOnEnableThrowsWhenSnapshotOlderThanTtl() throws Exception {
        MetadataManager manager = createMetadataManager();

        Map<String, String> props = new HashMap<>();
        props.put(ConfigOptions.TABLE_LOG_TTL.key(), Duration.ofHours(1).toString());
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(null).build().withProperties(props);

        long oldSnapshotTime = System.currentTimeMillis() - Duration.ofHours(2).toMillis();
        LakeSnapshotProvider.LakeSnapshotMetadata metadata =
                new LakeSnapshotProvider.LakeSnapshotMetadata(1L, oldSnapshotTime, "path");

        assertThatThrownBy(
                        () ->
                                invokeValidateTtlWindowOnEnable(
                                        manager,
                                        TablePath.of("db", "tbl"),
                                        descriptor,
                                        metadata))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("older than table.log.ttl");
    }

    private static MetadataManager createMetadataManager() {
        ZooKeeperClient zkClient = new ZooKeeperClient(null, null, null, 0, 0L, null) {
            @Override
            public long getTableIdAndIncrement() throws KeeperException, InterruptedException {
                return 1L;
            }

            @Override
            public void registerTable(TablePath tablePath, TableRegistration tableReg, boolean b) {}
        };
        Configuration conf = new Configuration();
        return new MetadataManager(zkClient, conf, null);
    }

    private static long invokeExtractTableIdFromFlussOffsets(
            MetadataManager manager, String value) throws Exception {
        java.lang.reflect.Method method =
                MetadataManager.class.getDeclaredMethod(
                        "extractTableIdFromFlussOffsets", String.class);
        method.setAccessible(true);
        return (Long) method.invoke(manager, value);
    }

    private static void invokeValidateFlussOffsetsConsistencyOnEnable(
            MetadataManager manager,
            TablePath tablePath,
            long tableId,
            @Nullable LakeSnapshotProvider.LakeSnapshotMetadata metadata)
            throws Exception {
        java.lang.reflect.Method method =
                MetadataManager.class.getDeclaredMethod(
                        "validateFlussOffsetsConsistencyOnEnable",
                        TablePath.class,
                        long.class,
                        LakeSnapshotProvider.LakeSnapshotMetadata.class);
        method.setAccessible(true);
        method.invoke(manager, tablePath, tableId, metadata);
    }

    private static void invokeValidateTtlWindowOnEnable(
            MetadataManager manager,
            TablePath tablePath,
            TableDescriptor descriptor,
            @Nullable LakeSnapshotProvider.LakeSnapshotMetadata metadata)
            throws Exception {
        java.lang.reflect.Method method =
                MetadataManager.class.getDeclaredMethod(
                        "validateTtlWindowOnEnable",
                        TablePath.class,
                        TableDescriptor.class,
                        LakeSnapshotProvider.LakeSnapshotMetadata.class);
        method.setAccessible(true);
        method.invoke(manager, tablePath, descriptor, metadata);
    }
}
