/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.query.impl.IndexRegistry;

/**
 * Visitor can inspect internal state of a node,
 * but it's not allowed to mutate its internal state.
 * <p>
 * Visitor can return a new instance of predicate
 * if modification is needed
 */
public interface Visitor {

    Predicate visit(EqualPredicate predicate, IndexRegistry indexes);

    Predicate visit(NotEqualPredicate predicate, IndexRegistry indexes);

    Predicate visit(AndPredicate predicate, IndexRegistry indexes);

    Predicate visit(OrPredicate predicate, IndexRegistry indexes);

    Predicate visit(NotPredicate predicate, IndexRegistry indexes);

    Predicate visit(InPredicate predicate, IndexRegistry indexes);

    Predicate visit(BetweenPredicate predicate, IndexRegistry indexes);

}
