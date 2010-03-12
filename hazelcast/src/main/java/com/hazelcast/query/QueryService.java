/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.query;

import com.hazelcast.impl.Node;
import com.hazelcast.impl.Record;
import com.hazelcast.util.UnboundedBlockingQueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class QueryService implements Runnable {

    private final Logger logger = Logger.getLogger(QueryService.class.getName());

    private final Node node;

    private final BlockingQueue<Runnable> queryQ = new UnboundedBlockingQueue<Runnable>();

    private boolean running = true;

    public QueryService(Node node) {
        this.node = node;
    }

    public void run() {
        while (running) {
            Runnable runnable;
            try {
                runnable = queryQ.poll(1, TimeUnit.SECONDS);
                if (runnable != null) {
                    runnable.run();
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        try {
            final CountDownLatch l = new CountDownLatch(1);
            queryQ.put(new Runnable() {
                public void run() {
                    running = false;
                    l.countDown();
                }
            });
            l.await();
        } catch (InterruptedException ignored) {
        }
    }

    public static long getLongValue(Object value) {
        if (value == null) return Long.MAX_VALUE;
        if (value instanceof Double) {
            return Double.doubleToLongBits((Double) value);
        } else if (value instanceof Float) {
            return Float.floatToIntBits((Float) value);
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof Boolean) {
            return (Boolean.TRUE.equals(value)) ? 1 : -1;
        } else {
            return value.hashCode();
        }
    }

    public void updateIndex(final MapIndexService mapIndexService, final Record record) {
        queryQ.offer(new Runnable () {
            public void run() {
                mapIndexService.index(record);
            }
        });
    }
}
