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

package com.hazelcast.test;

import com.google.common.collect.EvictingQueue;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.synchronizedCollection;
import static java.util.stream.Collectors.toList;

/**
 * Stores exceptions captured in tests when using {@link TestLoggerFactory}.
 */
public class ExceptionsCapturer {

    @SuppressWarnings("UnstableApiUsage")
    private static final Collection<Throwable> EXCEPTIONS = synchronizedCollection(EvictingQueue.create(100));

    public static List<Throwable> exceptionsLogged() {
        return new ArrayList<>(EXCEPTIONS);
    }

    @SafeVarargs
    public static List<Throwable> exceptionsOfTypes(@Nonnull Class<? extends Throwable>... exceptionClasses) {
        return exceptionsLogged()
                .stream()
                .filter(e -> {
                    for (Class<? extends Throwable> ec : exceptionClasses) {
                        if(ec.isAssignableFrom(e.getClass())) {
                            return true;
                        }
                    }
                    return false;
                })
                .collect(toList());
    }

    public static void log(Throwable thrown) {
        EXCEPTIONS.add(thrown);
    }
}
