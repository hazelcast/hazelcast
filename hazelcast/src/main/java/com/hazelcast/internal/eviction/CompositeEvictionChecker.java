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

package com.hazelcast.internal.eviction;

import com.hazelcast.internal.util.Preconditions;

/**
 * {@link EvictionChecker} implementation for composing
 * results of given {@link EvictionChecker} instances.
 */
public abstract class CompositeEvictionChecker
        implements EvictionChecker {

    protected final EvictionChecker[] evictionCheckers;

    protected CompositeEvictionChecker(EvictionChecker... evictionCheckers) {
        this.evictionCheckers = evictionCheckers;
    }

    /**
     * Operator for composing results of given {@link EvictionChecker} instances.
     */
    public enum CompositionOperator {

        /**
         * Result is <tt>true</tt> if results of <b>all</b> given {@link EvictionChecker} instances
         * have reached to max-size, otherwise <tt>false</tt>.
         */
        AND,

        /**
         * Result is <tt>true</tt> if result of <b>any</b> given {@link EvictionChecker} instances
         * have reached to max-size, otherwise <tt>false</tt>.
         */
        OR;

    }

    public static CompositeEvictionChecker newCompositeEvictionChecker(CompositionOperator compositionOperator,
                                                                       EvictionChecker... evictionCheckers) {
        Preconditions.isNotNull(compositionOperator, "composition");
        Preconditions.isNotNull(evictionCheckers, "evictionCheckers");
        if (evictionCheckers.length == 0) {
            throw new IllegalArgumentException("EvictionCheckers cannot be empty!");
        }
        switch (compositionOperator) {
            case AND:
                return new CompositeEvictionCheckerWithAndComposition(evictionCheckers);
            case OR:
                return new CompositeEvictionCheckerWithOrComposition(evictionCheckers);
            default:
                throw new IllegalArgumentException("Invalid composition operator: " + compositionOperator);
        }
    }

    private static final class CompositeEvictionCheckerWithAndComposition
            extends CompositeEvictionChecker {

        private CompositeEvictionCheckerWithAndComposition(EvictionChecker... evictionCheckers) {
            super(evictionCheckers);
        }

        @Override
        public boolean isEvictionRequired() {
            for (EvictionChecker evictionChecker : evictionCheckers) {
                if (!evictionChecker.isEvictionRequired()) {
                    return false;
                }
            }
            return true;
        }

    }

    private static final class CompositeEvictionCheckerWithOrComposition
            extends CompositeEvictionChecker {

        private CompositeEvictionCheckerWithOrComposition(EvictionChecker... evictionCheckers) {
            super(evictionCheckers);
        }

        @Override
        public boolean isEvictionRequired() {
            for (EvictionChecker evictionChecker : evictionCheckers) {
                if (evictionChecker.isEvictionRequired()) {
                    return true;
                }
            }
            return false;
        }

    }

}
