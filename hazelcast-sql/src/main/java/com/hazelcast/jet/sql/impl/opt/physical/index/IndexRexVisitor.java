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

package com.hazelcast.jet.sql.impl.opt.physical.index;

import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;

/**
 * Visitor that checks whether the given expression is valid for index filter creation.
 * <p>
 * Consider the expression {@code a > exp}. If there is an index on the column {@code [a]}, then
 * it can be used only if the {@code exp} will produce the same result for all rows. That is, it
 * cannot refer to any other columns.
 * <p>
 * We also filter out more complex constructs, where we are not 100% sure that they are context
 * independent.
 * <p>
 * For example, for the original filter {@code a > ? + 1}, the expression {@code ? + 1} is valid for
 * index lookup. To contrast, for the original filter {@code a > b + 1}, the expression {@code b + 1}
 * is not valid.
 */
final class IndexRexVisitor extends RexVisitorImpl<Void> {

    private boolean valid = true;

    private IndexRexVisitor() {
        super(true);
    }

    /**
     * @return {@code true} if passed rex could be used as index condition, {@code false} otherwise
     */
    static boolean isValid(RexNode rex) {
        IndexRexVisitor visitor = new IndexRexVisitor();

        rex.accept(visitor);

        return visitor.valid;
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef) {
        return markInvalid();
    }

    @Override
    public Void visitLocalRef(RexLocalRef localRef) {
        return markInvalid();
    }

    @Override
    public Void visitOver(RexOver over) {
        return markInvalid();
    }

    @Override
    public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
        return markInvalid();
    }

    @Override
    public Void visitRangeRef(RexRangeRef rangeRef) {
        return markInvalid();
    }

    @Override
    public Void visitFieldAccess(RexFieldAccess fieldAccess) {
        return markInvalid();
    }

    @Override
    public Void visitSubQuery(RexSubQuery subQuery) {
        return markInvalid();
    }

    @Override
    public Void visitTableInputRef(RexTableInputRef ref) {
        return markInvalid();
    }

    @Override
    public Void visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return markInvalid();
    }

    private Void markInvalid() {
        valid = false;

        return null;
    }
}
