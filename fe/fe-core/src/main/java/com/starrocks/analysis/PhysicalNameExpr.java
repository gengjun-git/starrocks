package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Table;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.SqlParser;

import java.util.List;

public class PhysicalNameExpr {
    private final Expr expr;

    private PhysicalNameExpr(Expr expr) {
        this.expr = expr;
    }

    public static PhysicalNameExpr create(Table table, Expr expr) {
        Expr clonedExpr = expr.clone();
        setPhysicalColumnName(table, clonedExpr);
        return new PhysicalNameExpr(expr);
    }

    // Only used on create table, you should make sure that no columns in expr have been renamed.
    public static PhysicalNameExpr create(Expr expr) {
        Expr clonedExpr = expr.clone();
        setPhysicalColumnNameToColumnName(expr);
        return new PhysicalNameExpr(expr);
    }

    public Expr getExpr(Table table) {
        Expr clonedExpr = expr.clone();
        setColumnName(table, clonedExpr);
        return clonedExpr;
    }

    public Expr getExpr(List<Column> schema) {
        Expr clonedExpr = expr.clone();
        setColumnName(schema, clonedExpr);
        return clonedExpr;
    }

    public String serialize() {
        return new ExprSerializeVisitor().visit(expr);
    }

    // only used for deserialization
    public static PhysicalNameExpr deserialize(String sql) {
        Expr expr = SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT);
        return new PhysicalNameExpr(setPhysicalColumnNameToColumnName(expr);
    }

    private void setColumnName(Table table, Expr expr) {
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            Column column = table.getColumn(slotRef.getColumnId());
            if (column == null) {
                throw new SemanticException(String.format("can not get column by physical name: %s, from table: %s",
                        slotRef.getColumnId(), table.getName()));
            }
            slotRef.setColumnName(column.getName());
        }

        for (Expr child : expr.getChildren()) {
            setColumnName(table, child);
        }
    }

    private void setColumnName(List<Column> schema, Expr expr) {
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            boolean found = false;
            for (Column column : schema) {
                if (column.getColumnId().equalsIgnoreCase(slotRef.getColumnId())) {
                    slotRef.setColumnName(column.getName());
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new SemanticException("can not get column by physical name: " + slotRef.getColumnId());
            }
        }

        for (Expr child : expr.getChildren()) {
            setColumnName(schema, child);
        }
    }

    private static void setPhysicalColumnName(Table table, Expr expr) {
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            Column column = table.getColumn(slotRef.getColumnName());
            if (column == null) {
                throw new SemanticException(String.format("can not get column: %s, from table: %s",
                        slotRef.getColumnName(), table.getName()));
            }
            slotRef.setColumnId(column.getColumnId());
        }

        for (Expr child : expr.getChildren()) {
            setPhysicalColumnName(table, child);
        }
    }

    private static void setPhysicalColumnNameToColumnName(Expr expr) {
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            slotRef.setColumnId(ColumnId.create(slotRef.getColumnName()));
        }

        for (Expr child : expr.getChildren()) {
            setPhysicalColumnNameToColumnName(child);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj.getClass() != PhysicalNameExpr.class) {
            return false;
        }

        return this.expr.equals(((PhysicalNameExpr) obj).expr);
    }

    private static class ExprSerializeVisitor extends AstToStringBuilder.AST2StringBuilderVisitor {
        @Override
        public String visitSlot(SlotRef node, Void context) {
            if (node.getTblNameWithoutAnalyzed() != null) {
                return node.getTblNameWithoutAnalyzed().toString() + "." + node.getColumnId();
            } else {
                return node.getColumnId().toString();
            }
        }
    }
}
