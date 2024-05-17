package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.SqlParser;

import java.util.List;
import java.util.Map;

public class PhysicalNameExpr {
    private final Expr expr;

    private PhysicalNameExpr(Expr expr) {
        this.expr = expr;
    }

    public static PhysicalNameExpr create(Map<String, Column> nameToColumn, Expr expr) {
        Expr clonedExpr = expr.clone();
        setPhysicalColumnName(nameToColumn, clonedExpr);
        return new PhysicalNameExpr(clonedExpr);
    }

    public static PhysicalNameExpr create(List<Column> schema, Expr expr) {
        Expr clonedExpr = expr.clone();
        setPhysicalColumnName(MetaUtils.convertToNameToColumn(schema), clonedExpr);
        return new PhysicalNameExpr(clonedExpr);
    }

    // Only used on create table, you should make sure that no columns in expr have been renamed.
    public static PhysicalNameExpr create(Expr expr) {
        Expr clonedExpr = expr.clone();
        setPhysicalColumnNameToColumnName(clonedExpr);
        return new PhysicalNameExpr(clonedExpr);
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

    public static PhysicalNameExpr deserialize(String sql) {
        Expr expr = SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT);
        setPhysicalColumnNameToColumnName(expr);
        return new PhysicalNameExpr(expr);
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

    private static void setPhysicalColumnName(Map<String, Column> nameToColumn, Expr expr) {
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            Column column = nameToColumn.get(slotRef.getColumnName());
            if (column == null) {
                throw new SemanticException(String.format("can not get column by name : %s", slotRef.getColumnName()));
            }
            slotRef.setColumnId(column.getColumnId());
        }

        for (Expr child : expr.getChildren()) {
            setPhysicalColumnName(nameToColumn, child);
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
                return node.getTblNameWithoutAnalyzed().toString() + "." + node.getColumnId().getId();
            } else {
                return node.getColumnId().getId();
            }
        }
    }
}
