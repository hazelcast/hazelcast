/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.maxsize.impl;

import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.util.Preconditions;

/**
 * {@link MaxSizeChecker} implementation for composing
 * results of given {@link MaxSizeChecker} instances.
 */
public abstract class CompositeMaxSizeChecker implements MaxSizeChecker {

    protected final MaxSizeChecker[] maxSizeCheckers;

    protected CompositeMaxSizeChecker(MaxSizeChecker... maxSizeCheckers) {
        this.maxSizeCheckers = maxSizeCheckers;
    }

    /**
     * Operator for composing results of given {@link MaxSizeChecker} instances.
     */
    public static enum CompositionOperator {

        /**
         * Result is <tt>true</tt> if results of <b>all</b> given {@link MaxSizeChecker} instances
         * have reached to max-size, otherwise <tt>false</tt>.
         */
        AND,

        /**
         * Result is <tt>true</tt> if result of <b>any</b> given {@link MaxSizeChecker} instances
         * have reached to max-size, otherwise <tt>false</tt>.
         */
        OR;

    }

    public static CompositeMaxSizeChecker newCompositeMaxSizeChecker(CompositionOperator compositionOperator,
                                                                     MaxSizeChecker... maxSizeCheckers) {
        Preconditions.isNotNull(compositionOperator, "Composition operator cannot be null!");
        Preconditions.isNotNull(maxSizeCheckers, "MaxSizeChecker's cannot be null!");
        if (maxSizeCheckers.length == 0) {
            throw new IllegalArgumentException("MaxSizeChecker's cannot be empty!");
        }
        switch (compositionOperator) {
            case AND:
                return new CompositeMaxSizeCheckerWithAndComposition(maxSizeCheckers);
            case OR:
                return new CompositeMaxSizeCheckerWithOrComposition(maxSizeCheckers);
            default:
                throw new IllegalArgumentException("Invalid composition operator: " + compositionOperator);
        }
    }

    private static final class CompositeMaxSizeCheckerWithAndComposition extends CompositeMaxSizeChecker {

        private CompositeMaxSizeCheckerWithAndComposition(MaxSizeChecker... maxSizeCheckers) {
            super(maxSizeCheckers);
        }

        @Override
        public boolean isReachedToMaxSize() {
            for (MaxSizeChecker maxSizeChecker : maxSizeCheckers) {
                if (!maxSizeChecker.isReachedToMaxSize()) {
                    return false;
                }
            }
            return true;
        }

    }

    private static final class CompositeMaxSizeCheckerWithOrComposition extends CompositeMaxSizeChecker {

        private CompositeMaxSizeCheckerWithOrComposition(MaxSizeChecker... maxSizeCheckers) {
            super(maxSizeCheckers);
        }

        @Override
        public boolean isReachedToMaxSize() {
            for (MaxSizeChecker maxSizeChecker : maxSizeCheckers) {
                if (maxSizeChecker.isReachedToMaxSize()) {
                    return true;
                }
            }
            return false;
        }

    }

}
