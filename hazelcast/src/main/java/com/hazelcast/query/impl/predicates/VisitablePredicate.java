/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
 * Predicates which can be visited by optimizer.
 *
 */
public interface VisitablePredicate {

    /**
     * Accept visitor. Predicate can either return it's own instance if no modification
     * was done as a result of the visit. In the case there is a change needed then
     * the predicate has to return changed copy of itself. Predicates has to be treated
     * as immutable for optimization purposes.
     *
     * @param visitor visitor to accept
     * @param indexes indexes
     * @return itself or its changed copy
     */
    Predicate accept(Visitor visitor, Indexes indexes);
}
