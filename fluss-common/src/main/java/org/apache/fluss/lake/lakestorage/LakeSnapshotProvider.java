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

package org.apache.fluss.lake.lakestorage;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

/**
 * Provides snapshot metadata for lake tables.
 *
 * <p>This is an optional SPI. Implementations of {@link LakeCatalog} may choose to also
 * implement this interface to expose additional snapshot information for stricter validation
 * when enabling datalake for a Fluss table.
 */
@PublicEvolving
public interface LakeSnapshotProvider {

    /**
     * Get the latest snapshot metadata of the given table in the lake.
     *
     * @param tablePath the table path
     * @return the latest snapshot metadata, or {@code null} if the table has no snapshot yet
     * @throws TableNotExistException if the table does not exist in the lake
     */
    @Nullable
    LakeSnapshotMetadata getLatestSnapshotMetadata(TablePath tablePath)
            throws TableNotExistException;

    /** Metadata of the latest snapshot in lake for a table. */
    final class LakeSnapshotMetadata {

        private final long snapshotId;
        private final long commitTimeMillis;

        /**
         * The value of the {@code fluss-offsets} property stored in the lake snapshot properties.
         * It may be {@code null} if the snapshot was not produced by Fluss tiering.
         */
        @Nullable private final String flussOffsetsProperty;

        public LakeSnapshotMetadata(
                long snapshotId, long commitTimeMillis, @Nullable String flussOffsetsProperty) {
            this.snapshotId = snapshotId;
            this.commitTimeMillis = commitTimeMillis;
            this.flussOffsetsProperty = flussOffsetsProperty;
        }

        public long getSnapshotId() {
            return snapshotId;
        }

        public long getCommitTimeMillis() {
            return commitTimeMillis;
        }

        @Nullable
        public String getFlussOffsetsProperty() {
            return flussOffsetsProperty;
        }
    }
}
