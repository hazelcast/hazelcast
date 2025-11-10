/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.jet;

import com.hazelcast.jet.function.RunnableEx;

import javax.annotation.Nonnull;
import java.lang.ref.Cleaner;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Simple utility that produces some data in background thread and returns how many items were produced.
 */
public class DataProducer {

    private final String jdbcUrl;
    private final AtomicInteger producedItems = new AtomicInteger();
    private final AtomicInteger nextId = new AtomicInteger(100_001);
    private Throwable error;

    private volatile ScheduledFuture<?> scheduledFuture;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Semaphore semaphore = new Semaphore(1);

    private final Cleaner cleaner = Cleaner.create();
    private volatile Connection connection;
    private int initialDelay = 0;
    private int delay = 40;
    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;

    private final Function<Integer, String> sqlProducer;

    private volatile boolean running;

    public DataProducer(@Nonnull String jdbcUrl, @Nonnull Function<Integer, String> sqlProducer) {
        this.jdbcUrl = jdbcUrl;
        this.sqlProducer = sqlProducer;
    }

    public DataProducer setDelays(int initialDelay, int delay, TimeUnit timeUnit) {
        this.initialDelay = initialDelay;
        this.delay = delay;
        this.timeUnit = timeUnit;
        return this;
    }

    public void start() {
        running = true;
        scheduledFuture = scheduler.scheduleWithFixedDelay(unchecked(() -> {
            if (running) {
                semaphore.acquire();
                try {
                    makeSureConnectionUp();
                    int id = nextId.getAndIncrement();
                    int updated = connection
                            .prepareStatement(sqlProducer.apply(id))
                            .executeUpdate();
                    assert updated == 1;
                    producedItems.incrementAndGet();
                    connection.commit();
                } catch (Throwable t) {
                    connection.rollback();
                    error = t;
                } finally {
                    semaphore.release();
                }
            }
        }), initialDelay, delay, timeUnit);
    }

    private void makeSureConnectionUp() {
        if (connection == null) {
            try {
                connection = DriverManager.getConnection(jdbcUrl);
                connection.setAutoCommit(false);
                cleaner.register(connection, unchecked(() -> connection.close()));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public int stop() {
        running = false;
        try {
            semaphore.acquire();
            if (error != null) {
                throw new RuntimeException(error);
            }
            return producedItems.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            semaphore.release();
            cancelFuture();
        }
    }

    private void cancelFuture() {
        try {
            do {
                scheduledFuture.cancel(true);
            } while (!scheduledFuture.isCancelled());
        } catch (Exception ignored) {
        }
    }

    private static RunnableEx unchecked(RunnableEx function) {
        return function;
    }
}
