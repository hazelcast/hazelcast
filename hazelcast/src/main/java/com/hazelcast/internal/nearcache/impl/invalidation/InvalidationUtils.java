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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.spi.impl.eventservice.EventRegistration;

import java.util.function.Function;

import static java.lang.Boolean.TRUE;

/**
 * Utils for invalidations.
 */
public final class InvalidationUtils {

    public static final long NO_SEQUENCE = -1L;
    public static final Function<EventRegistration, Boolean> TRUE_FILTER = new TrueFilter();

    private InvalidationUtils() {
    }

    private static class TrueFilter implements Function<EventRegistration, Boolean> {
        @Override
        public Boolean apply(EventRegistration eventRegistration) {
            return TRUE;
        }
    }
}
