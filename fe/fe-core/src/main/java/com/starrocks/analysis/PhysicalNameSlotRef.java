package com.starrocks.analysis;

import com.google.common.base.Objects;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Table;
import com.starrocks.sql.analyzer.SemanticException;

public class PhysicalNameSlotRef extends SlotRef {
    private final ColumnId columnId;
    private Table table;

    public PhysicalNameSlotRef(SlotRef slotRef, ColumnId columnId) {
        super(slotRef);
        this.columnId = columnId;
    }

    public PhysicalNameSlotRef(PhysicalNameSlotRef other) {
        super(other);
        this.columnId = other.columnId;
        this.table = other.table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public String getColumnName() {
        if (table == null) {
            throw new SemanticException("table can not be null for PhysicalNameSlotRef");
        }
        Column column = table.getColumn(columnId);
        if (column == null) {
            throw new SemanticException("can not find column by physical name: " + columnId
                    + ", in table: " + table.getName());
        }
        return column.getName();
    }

    public ColumnId getColumnId() {
        return columnId;
    }

    @Override
    public Expr clone() {
        return new PhysicalNameSlotRef(this);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), columnId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }

        PhysicalNameSlotRef other = (PhysicalNameSlotRef) obj;
        if ((columnId == null) != (other.columnId == null)) {
            return false;
        }

        return columnId == null || columnId.equalsIgnoreCase(other.columnId);
    }
}
