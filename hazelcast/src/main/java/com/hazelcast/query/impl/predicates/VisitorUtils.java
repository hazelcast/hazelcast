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

import static com.hazelcast.internal.util.collection.ArrayUtils.createCopy;

public final class VisitorUtils {

    private VisitorUtils() {
    }

    /**
     * Accept visitor by all predicates. It treats the input array as immutable.
     *
     * It's Copy-On-Write: If at least one predicate returns a different instance
     * then this method returns a copy of the passed arrays.
     *
     * @param predicates
     * @param visitor
     * @return
     */
    public static Predicate[] acceptVisitor(Predicate[] predicates, Visitor visitor, Indexes indexes) {
        Predicate[] target = predicates;
        boolean copyCreated = false;
        for (int i = 0; i < predicates.length; i++) {
            Predicate predicate = predicates[i];
            if (predicate instanceof VisitablePredicate) {
                Predicate transformed = ((VisitablePredicate) predicate).accept(visitor, indexes);
                if (transformed != predicate) {
                    if (!copyCreated) {
                        copyCreated = true;
                        target = createCopy(target);
                    }
                    target[i] = transformed;
                }
            }
        }
        return target;
    }
}
