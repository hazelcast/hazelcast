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

package com.hazelcast.sql.impl.calcite.opt.physical.index;

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

public final class IndexRexVisitor extends RexVisitorImpl<Void> {

    private boolean valid = true;

    private IndexRexVisitor() {
        super(true);
    }

    /**
     * @return {@code true} if passed rex could be used as index condition, {@code false} otherwise
     */
    public static boolean isValid(RexNode rex) {
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

    @Override public Void visitTableInputRef(RexTableInputRef ref) {
        return markInvalid();
    }

    @Override public Void visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return markInvalid();
    }

    private Void markInvalid() {
        valid = false;

        return null;
    }
}
