// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.catalog;


import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.ColumnIdExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.persist.ExpressionSerializedObject;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.sql.ast.AstVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * ExpressionRangePartitionInfo replace columns with expressions
 * Some Descriptions:
 * 1. no overwrite old serialized method: read、write and readFields, because we use gson now
 * 2. As of 2023-09, it's still used to describe auto range using expr like PARTITION BY date_trunc('day', col).
 */
@Deprecated
public class ExpressionRangePartitionInfo extends RangePartitionInfo implements GsonPreProcessable, GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(ExpressionRangePartitionInfo.class);

    public static final String AUTOMATIC_SHADOW_PARTITION_NAME = "$shadow_automatic_partition";
    public static final String SHADOW_PARTITION_PREFIX = "$";

    private List<ColumnIdExpr> partitionExprs;

    @SerializedName(value = "partitionExprs")
    private List<ExpressionSerializedObject> serializedPartitionExprs;

    public ExpressionRangePartitionInfo() {
        this.type = PartitionType.EXPR_RANGE;
    }

    @Override
    public void gsonPreProcess() throws IOException {
        super.gsonPreProcess();
        List<ExpressionSerializedObject> serializedPartitionExprs = Lists.newArrayList();
        for (ColumnIdExpr partitionExpr : partitionExprs) {
            if (partitionExpr != null) {
                serializedPartitionExprs.add(ExpressionSerializedObject.create(partitionExpr));
            }
        }
        this.serializedPartitionExprs = serializedPartitionExprs;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        List<ColumnIdExpr> partitionExprs = Lists.newArrayList();
        for (ExpressionSerializedObject expressionSql : serializedPartitionExprs) {
            if (expressionSql.getExpressionSql() != null) {
                partitionExprs.add(expressionSql.deserialize());
            }
        }
        this.partitionExprs = partitionExprs;
    }

    public ExpressionRangePartitionInfo(List<ColumnIdExpr> partitionExprs, List<Column> columns, PartitionType type) {
        super(columns);
        Preconditions.checkState(partitionExprs != null);
        Preconditions.checkState(partitionExprs.size() > 0);
        Preconditions.checkState(partitionExprs.size() == columns.size());
        this.partitionExprs = partitionExprs;
        this.isMultiColumnPartition = partitionExprs.size() > 0;
        this.type = type;
    }

    public List<Expr> getPartitionExprs(Map<ColumnId, Column> idToColumn) {
        List<Expr> result = new ArrayList<>(partitionExprs.size());
        for (ColumnIdExpr columnIdExpr : partitionExprs) {
            result.add(columnIdExpr.convertToColumnNameExpr(idToColumn));
        }
        return result;
    }

    public List<Expr> getPartitionExprs(List<Column> schema) {
        List<Expr> result = new ArrayList<>(partitionExprs.size());
        for (ColumnIdExpr columnIdExpr : partitionExprs) {
            result.add(columnIdExpr.convertToColumnNameExpr(schema));
        }
        return result;
    }

    public int getPartitionExprsSize() {
        return partitionExprs.size();
    }

    @Override
    public String toSql(OlapTable table, List<Long> partitionId) {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY ");
        if (table instanceof MaterializedView) {
            sb.append("(");
            for (ColumnIdExpr columnIdExpr : partitionExprs) {
                Expr expr = columnIdExpr.convertToColumnNameExpr(table.getIdToColumn());
                if (expr instanceof SlotRef) {
                    SlotRef slotRef = (SlotRef) expr.clone();
                    sb.append("`").append(slotRef.getColumnName()).append("`").append(",");
                }
                if (expr instanceof FunctionCallExpr) {
                    Expr cloneExpr = expr.clone();
                    for (int i = 0; i < cloneExpr.getChildren().size(); i++) {
                        Expr child = cloneExpr.getChildren().get(i);
                        if (child instanceof SlotRef) {
                            cloneExpr.setChild(i, new SlotRef(null, ((SlotRef) child).getColumnName()));
                            break;
                        }
                    }
                    sb.append(cloneExpr.toSql()).append(",");
                }
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            return sb.toString();
        }
        sb.append(Joiner.on(", ").join(partitionExprs
                .stream()
                .map(physicalExpr -> physicalExpr.convertToColumnNameExpr(table).toSql())
                .collect(toList())));
        return sb.toString();
    }

    public static PartitionInfo read(DataInput in) throws IOException {
        return null;
    }

    /**
     * Do actions when rename referred table's db or table name.
     * @param dbName        : new db name which can be null or empty and will be not updated then.
     * @param newTableName  : new table name which must be not null or empty.
     */
    public void renameTableName(String dbName, String newTableName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(newTableName));
        AstVisitor<Void, Void> renameVisitor = new AstVisitor<Void, Void>() {
            @Override
            public Void visitExpression(Expr expr, Void context) {
                for (Expr child : expr.getChildren()) {
                    child.accept(this, context);
                }
                return null;
            }

            @Override
            public Void visitSlot(SlotRef node, Void context) {
                TableName tableName = node.getTblNameWithoutAnalyzed();
                if (tableName != null) {
                    if (!Strings.isNullOrEmpty(dbName)) {
                        tableName.setDb(dbName);
                    }
                    tableName.setTbl(newTableName);
                }
                return null;
            }
        };
        for (ColumnIdExpr expr : partitionExprs) {
            expr.getExpr().accept(renameVisitor, null);
        }
    }

    @Override
    public boolean isAutomaticPartition() {
        return type == PartitionType.EXPR_RANGE;
    }

}

