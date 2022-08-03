package com.hazelcast.jet.sql.impl.connector.jdbc;

import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * Visitor for {@link SqlNode} that collects indexes of query input parameters used in the SqlNode
 */
class ParamCollectingVisitor extends SqlBasicVisitor<SqlNode> {

    private final List<Integer> parameterList = new ArrayList<>();

    @Override
    public SqlNode visit(SqlDynamicParam param) {
        parameterList.add(param.getIndex());
        return param;
    }

    public List<Integer> parameterList() {
        return parameterList;
    }
}
