/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.parse;

import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.util.HashSet;
import java.util.Set;

/**
 * Visitor that throws exceptions for unsupported SQL features.
 */
public final class UnsupportedOperationVisitor implements SqlVisitor<Void> {

    public static final UnsupportedOperationVisitor INSTANCE = new UnsupportedOperationVisitor();

    /** Error messages. */
    private static final Resource RESOURCE = Resources.create(Resource.class);

    /** A set of {@link SqlKind} values that are supported without any additional validation. */
    private static final Set<SqlKind> SUPPORTED_KINDS;

    static {
        // We define all supported features explicitly instead of getting them from predefined sets of SqlKind class.
        // This is needed to ensure that we do not miss any unsupported features when something is added to a new version
        // of Apache Calcite.
        SUPPORTED_KINDS = new HashSet<>();

        // "IS" predicates
        SUPPORTED_KINDS.add(SqlKind.IS_NULL);

        // Comparisons predicates
        SUPPORTED_KINDS.add(SqlKind.EQUALS);
        SUPPORTED_KINDS.add(SqlKind.NOT_EQUALS);
        SUPPORTED_KINDS.add(SqlKind.LESS_THAN);
        SUPPORTED_KINDS.add(SqlKind.GREATER_THAN);
        SUPPORTED_KINDS.add(SqlKind.GREATER_THAN_OR_EQUAL);
        SUPPORTED_KINDS.add(SqlKind.LESS_THAN_OR_EQUAL);

        // Miscellaneous
        SUPPORTED_KINDS.add(SqlKind.AS);
    }

    private UnsupportedOperationVisitor() {
        // No-op.
    }

    @Override
    public Void visit(SqlCall call) {
        processCall(call);

        call.getOperator().acceptCall(this, call);

        return null;
    }

    @Override
    public Void visit(SqlNodeList nodeList) {
        if (nodeList.size() == 0) {
            return null;
        }

        for (int i = 0; i < nodeList.size(); i++) {
            SqlNode node = nodeList.get(i);

            node.accept(this);
        }

        return null;
    }

    @Override
    public Void visit(SqlIdentifier id) {
        return null;
    }

    @Override
    public Void visit(SqlDataTypeSpec type) {
        throw error(type, RESOURCE.custom("Type specification is not supported"));
    }

    @Override
    public Void visit(SqlDynamicParam param) {
        throw error(param, RESOURCE.custom("Parameters are not supported"));
    }

    @Override
    public Void visit(SqlLiteral literal) {
        SqlTypeName typeName = literal.getTypeName();

        switch (typeName) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
                return null;

            default:
                throw error(literal, RESOURCE.custom(typeName + " literals are not supported"));
        }
    }

    @Override
    public Void visit(SqlIntervalQualifier intervalQualifier) {
        return null;
    }

    private void processCall(SqlCall call) {
        SqlKind kind = call.getKind();

        if (SUPPORTED_KINDS.contains(kind)) {
            return;
        }

        switch (kind) {
            case SELECT:
                processSelect((SqlSelect) call);

                return;

            default:
                throw unsupported(call, call.getKind());
        }
    }

    private void processSelect(SqlSelect select) {
        if (select.hasOrderBy()) {
            throw unsupported(select.getOrderList(), SqlKind.ORDER_BY);
        }

        if (select.getGroup() != null && select.getGroup().size() > 0) {
            throw unsupported(select.getGroup(), "GROUP BY");
        }
    }

    private CalciteContextException unsupported(SqlNode node, SqlKind kind) {
        return unsupported(node, kind.sql.replace('_', ' '));
    }

    private CalciteContextException unsupported(SqlNode node, String name) {
        return error(node, RESOURCE.notSupported(name));
    }

    private CalciteContextException error(SqlNode node, Resources.ExInst<SqlValidatorException> err) {
        return SqlUtil.newContextException(node.getParserPosition(), err);
    }

    public interface Resource {
        @Resources.BaseMessage("{0}")
        Resources.ExInst<SqlValidatorException> custom(String a0);

        @Resources.BaseMessage("{0} is not supported")
        Resources.ExInst<SqlValidatorException> notSupported(String a0);
    }
}
