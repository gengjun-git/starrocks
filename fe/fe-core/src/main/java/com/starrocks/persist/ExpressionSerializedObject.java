package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.PhysicalNameExpr;

public class ExpressionSerializedObject {
    private ExpressionSerializedObject(String expressionSql) {
        this.expressionSql = expressionSql;
    }

    @SerializedName("expr")
    public String expressionSql;

    public static 

    public static ExpressionSerializedObject create(PhysicalNameExpr expr) {
        return new ExpressionSerializedObject(expr.serialize());
    }

    public PhysicalNameExpr deserialize() {
        return PhysicalNameExpr.deserialize(expressionSql);
    }
}
