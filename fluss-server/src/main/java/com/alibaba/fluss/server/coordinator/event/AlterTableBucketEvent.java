package com.alibaba.fluss.server.coordinator.event;

import com.alibaba.fluss.server.zk.data.TableAssignment;

import java.util.Objects;

/** An event for alter table bucket. */
public class AlterTableBucketEvent implements CoordinatorEvent {

    private final long tableId;
    private final TableAssignment tableAssignment;

    public AlterTableBucketEvent(long tableId, TableAssignment tableAssignment) {
        this.tableId = tableId;
        this.tableAssignment = tableAssignment;
    }

    public long getTableId() {
        return tableId;
    }

    public TableAssignment getTableAssignment() {
        return tableAssignment;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AlterTableBucketEvent that = (AlterTableBucketEvent) o;
        return Objects.equals(tableAssignment, that.tableAssignment);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableAssignment);
    }

    @Override
    public String toString() {
        return "AlterTableBucketEvent{" + "tableAssignment=" + tableAssignment + '}';
    }
}
