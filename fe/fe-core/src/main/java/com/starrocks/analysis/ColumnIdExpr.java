package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.SqlParser;

import java.util.List;
import java.util.Map;

public class ColumnIdExpr {
    private final Expr expr;

    private ColumnIdExpr(Expr expr) {
        this.expr = expr;
    }

    public static ColumnIdExpr create(Map<String, Column> nameToColumn, Expr expr) {
        Expr clonedExpr = expr.clone();
        setColumnId(nameToColumn, clonedExpr);
        return new ColumnIdExpr(clonedExpr);
    }

    public static ColumnIdExpr create(List<Column> schema, Expr expr) {
        Expr clonedExpr = expr.clone();
        setColumnId(MetaUtils.convertToNameToColumn(schema), clonedExpr);
        return new ColumnIdExpr(clonedExpr);
    }

    // Only used on create table, you should make sure that no columns in expr have been renamed.
    public static ColumnIdExpr create(Expr expr) {
        Expr clonedExpr = expr.clone();
        setColumnIdToColumnName(clonedExpr);
        return new ColumnIdExpr(clonedExpr);
    }

    public Expr convertToColumnNameExpr(Map<ColumnId, Column> idToColumn) {
        Expr clonedExpr = expr.clone();
        setColumnName(idToColumn, clonedExpr);
        return clonedExpr;
    }

    public Expr convertToColumnNameExpr(List<Column> schema) {
        Expr clonedExpr = expr.clone();
        setColumnName(MetaUtils.convertToIdToColumn(schema), clonedExpr);
        return clonedExpr;
    }

    public Expr getExpr() {
        return expr;
    }

    public String serialize() {
        return new ExprSerializeVisitor().visit(expr);
    }

    public static ColumnIdExpr deserialize(String sql) {
        Expr expr = SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT);
        setColumnIdToColumnName(expr);
        return new ColumnIdExpr(expr);
    }

    private void setColumnName(Map<ColumnId, Column> idToColumn, Expr expr) {
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            Column column = idToColumn.get(slotRef.getColumnId());
            if (column == null) {
                throw new SemanticException(String.format("can not get column by column id: %s", slotRef.getColumnId()));
            }
            slotRef.setColumnName(column.getName());
        }

        for (Expr child : expr.getChildren()) {
            setColumnName(idToColumn, child);
        }
    }

    private static void setColumnId(Map<String, Column> nameToColumn, Expr expr) {
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            Column column = nameToColumn.get(slotRef.getColumnName());
            if (column == null) {
                throw new SemanticException(String.format("can not get column by name : %s", slotRef.getColumnName()));
            }
            slotRef.setColumnId(column.getColumnId());
        }

        for (Expr child : expr.getChildren()) {
            setColumnId(nameToColumn, child);
        }
    }

    private static void setColumnIdToColumnName(Expr expr) {
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            slotRef.setColumnId(ColumnId.create(slotRef.getColumnName()));
        }

        for (Expr child : expr.getChildren()) {
            setColumnIdToColumnName(child);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj.getClass() != ColumnIdExpr.class) {
            return false;
        }

        return this.expr.equals(((ColumnIdExpr) obj).expr);
    }

    private static class ExprSerializeVisitor extends AstToStringBuilder.AST2StringBuilderVisitor {
        @Override
        public String visitSlot(SlotRef node, Void context) {
            if (node.getTblNameWithoutAnalyzed() != null) {
                return node.getTblNameWithoutAnalyzed().toString() + "." + node.getColumnId().getId();
            } else {
                return node.getColumnId().getId();
            }
        }
    }
}
