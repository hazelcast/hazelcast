/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.validate;

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.parse.QueryParseResult;
import com.hazelcast.jet.sql.impl.parse.QueryParser;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.ViewResolver;
import com.hazelcast.sql.impl.schema.view.View;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

/**
 * Hazelcast view expander helper class.
 */
public class HazelcastViewExpander {
    private final HazelcastSqlValidator validator;
    private final Set<View> visitedViews = new HashSet<>();
    private final QueryParser parser;

    public HazelcastViewExpander(HazelcastSqlValidator validator) {
        this.validator = validator;
        this.parser = new QueryParser(validator);
    }

    /**
     * Tries to detect and expand view in given {@link SqlSelect} argument.
     * View expanding happens on AST IR level.
     */
    @SuppressWarnings("CheckStyle")
    public void expandView(SqlSelect selectCall) {
        if (selectCall.getFrom() == null) {
            return;
        }

        SqlNode from = selectCall.getFrom();
        ViewResolver viewResolver = validator.getViewResolver();
        View resolvedView = null;

        if (from instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) from;
            SqlOperator operator = call.getOperator();
            if (operator instanceof SqlAsOperator) {
                resolvedView = extractView((SqlIdentifier) call.getOperandList().get(1), viewResolver);
            }
        }

        if (from instanceof SqlIdentifier) {
            SqlIdentifier fromClause = (SqlIdentifier) from;
            resolvedView = extractView(fromClause, viewResolver);
        }

        if (from instanceof SqlJoin) {
            SqlJoin joinFrom = (SqlJoin) from;
            if (joinFrom.getLeft() instanceof SqlIdentifier) {
                SqlIdentifier left = (SqlIdentifier) joinFrom.getLeft();
                resolvedView = extractView(left, viewResolver);
                if (resolvedView != null) {
                    joinFrom.setLeft(parser.parse(resolvedView.query()).getNode());
                }
            }

            if (joinFrom.getRight() instanceof SqlIdentifier) {
                SqlIdentifier right = (SqlIdentifier) joinFrom.getRight();
                resolvedView = extractView(right, viewResolver);
                if (resolvedView != null) {
                    joinFrom.setRight(parser.parse(resolvedView.query()).getNode());
                }
            }
            return;
        }

        if (visitedViews.contains(resolvedView)) {
            throw QueryException.error("Infinite recursion during view expanding detected");
        }

        if (resolvedView != null) {
            visitedViews.add(resolvedView);
            // Note: despite query was parsed & validated previously,
            // we may expect dependent mapping to be removed.
            try {
                QueryParseResult parseResult = parser.parse(resolvedView.query());
                selectCall.setFrom(parseResult.getNode());
                rewriteProjectionOwnerTable(selectCall);
            } catch (QueryException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * a
     *
     * @param fromClause   AST node.
     * @param viewResolver view resolver.
     * @return view instance if resolved, {@code null}, if not.
     */
    @Nullable
    private View extractView(SqlIdentifier fromClause, ViewResolver viewResolver) {
        if (!ValidationUtil.isCatalogObjectNameValid(fromClause)) {
            // We are not throwing any exceptions here, delegating it to validation stage.
            return null;
        }
        String id = fromClause.names.get(fromClause.names.size() - 1);
        return viewResolver.resolve(id);
    }

    /**
     * During multi-level view expansion it may happens,
     * that projection contains view aliases in its scope.
     * <p>
     * Example : {@code SELECT v.this FROM (SELECT __key, this FROM map}.
     * <p>
     * Here, {@code v} is not in query scope, and should be rewritten with actual scope.
     * Since Calcite does scope management by itself,
     * we just manually remove old scopes from identifiers.
     * <p>
     * After the rewrite, query will have have a look like:
     * {@code SELECT this FROM (SELECT __key, this FROM map}.
     *
     * @param selectCall SELECT AST node to perform rewrite of old projections owners
     */
    private void rewriteProjectionOwnerTable(SqlSelect selectCall) {
        SqlNodeList selectList = selectCall.getSelectList();
        for (SqlNode node : selectList) {
            if (node instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) node;
                id.setNames(ImmutableList.of(id.names.get(id.names.size() - 1)), null);
            }

            // handling function projections, like
            // SELECT SUM('v.this') ... -> SELECT SUM(this) ...
            if (node instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) node;
                for (SqlNode subNode : call.getOperandList()) {
                    if (subNode instanceof SqlIdentifier) {
                        SqlIdentifier id = (SqlIdentifier) subNode;
                        id.setNames(ImmutableList.of(id.names.get(id.names.size() - 1)), null);
                    }
                }
            }
        }
    }
}
