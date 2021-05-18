/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.opt;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;

import java.io.Serializable;

/**
 * Serializable equivalent of {@link RelFieldCollation}.
 */
public class FieldCollation implements Serializable {
    private final int index;
    private final Direction direction;
    private final NullDirection nullDirection;

    public FieldCollation(RelFieldCollation coll) {
        index = coll.getFieldIndex();
        direction = coll.getDirection();
        nullDirection = coll.nullDirection;
    }

    public int getIndex() {
        return index;
    }

    public Direction getDirection() {
        return direction;
    }

    public NullDirection getNullDirection() {
        return nullDirection;
    }
}
