/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Equality predicate.
 */
public class EqualsPredicate implements Predicate {
    /** Left expression. */
    private Expression left;

    /** Right expression. */
    private Expression right;

    public EqualsPredicate() {
        // No-op.
    }

    public EqualsPredicate(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public Boolean eval(QueryContext ctx, Row row) {
        Object leftObj = left.eval(ctx, row);
        Object rightObj = right.eval(ctx, row);

        return leftObj == null ? rightObj == null : leftObj.equals(rightObj);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(left);
        out.writeObject(right);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        left = in.readObject();
        right = in.readObject();
    }
}
