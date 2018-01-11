/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicates;


import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Indexes;

/**
 * Visitor can inspect internal state of a node,
 * but it's not allowed to mutate its internal state.
 *
 * Visitor can return a new instance of predicate
 * if modification is needed
 *
 *
 */
public interface Visitor {
    Predicate visit(AndPredicate predicate, Indexes indexes);

    Predicate visit(OrPredicate predicate, Indexes indexes);

    Predicate visit(NotPredicate predicate, Indexes indexes);
}
