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

package com.hazelcast.test;

import com.google.common.collect.EvictingQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.logging.LogListener;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;

import static java.util.Collections.synchronizedCollection;
import static java.util.stream.Collectors.toList;

/**
 * Records exceptions logged by HazelcastInstance via the logging service.
 * <p>
 * To use create a new instance and pass the instances you want to track the exceptions for.
 * <p>
 * NOTE: You can't use this class if you share the HazelcastInstances between tests in
 * {@link HazelcastParallelClassRunner}.
 */
public class ExceptionRecorder implements LogListener {

    private final HazelcastInstance[] instances;

    @SuppressWarnings("UnstableApiUsage")
    private final Collection<Throwable> throwables = synchronizedCollection(EvictingQueue.create(100));

    public ExceptionRecorder(HazelcastInstance instance, Level level) {
        this(new HazelcastInstance[]{instance}, level);
    }

    public ExceptionRecorder(HazelcastInstance[] instances, Level level) {
        this.instances = instances;
        for (HazelcastInstance instance : this.instances) {
            instance.getLoggingService().addLogListener(level, this);
        }
    }

    /**
     * Unregister the recorder.
     * Use e.g. when you want to track exceptions only for a single test,
     * but you share the instance between multiple tests.
     */
    public void removeListener() {
        for (HazelcastInstance instance : instances) {
            instance.getLoggingService().removeLogListener(this);
        }
    }

    public List<Throwable> exceptionsLogged() {
        return new ArrayList<>(throwables);
    }

    @SafeVarargs
    public final List<Throwable> exceptionsOfTypes(@Nonnull Class<? extends Throwable>... exceptionClasses) {
        return exceptionsLogged()
                .stream()
                .filter(e -> {
                    for (Class<? extends Throwable> ec : exceptionClasses) {
                        if (ec.isAssignableFrom(e.getClass())) {
                            return true;
                        }
                    }
                    return false;
                })
                .collect(toList());
    }

    @Override
    public void log(LogEvent logEvent) {
        Throwable throwable = logEvent.getLogRecord().getThrown();
        if (throwable != null) {
            throwables.add(throwable);
        }
    }

    /**
     * Clears the recorded exceptions, should be used between tests
     */
    public void clear() {
        throwables.clear();
    }
}
