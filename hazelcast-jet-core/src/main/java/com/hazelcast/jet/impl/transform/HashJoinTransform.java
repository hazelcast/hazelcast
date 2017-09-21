/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.transform;

import com.hazelcast.jet.JoinClause;
import com.hazelcast.jet.datamodel.Tag;

import javax.annotation.Nonnull;
import java.util.List;

public class HashJoinTransform<E0> implements MultiTransform {
    private final List<JoinClause<?, E0, ?, ?>> clauses;
    private final List<Tag> tags;

    public HashJoinTransform(@Nonnull List<JoinClause<?, E0, ?, ?>> clauses, @Nonnull List<Tag> tags) {
        this.clauses = clauses;
        this.tags = tags;
    }

    public List<JoinClause<?, E0, ?, ?>> clauses() {
        return clauses;
    }

    public List<Tag> tags() {
        return tags;
    }

    @Override
    public String toString() {
        return tags.size() + "-way HashJoin";
    }
}
