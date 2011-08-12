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

package com.hazelcast.impl;

import com.hazelcast.logging.ILogger;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class ExecutorThreadFactory implements ThreadFactory {
    final ILogger logger;
    final ThreadGroup group;
    final AtomicInteger threadNumber = new AtomicInteger(1);
    final String namePrefix;
    final ClassLoader classLoader;
    final Node node;

    public ExecutorThreadFactory(Node node, String threadNamePrefix, ClassLoader classLoader) {
        this.node = node;
        this.group = node.threadGroup;
        this.classLoader = classLoader;
        this.namePrefix = threadNamePrefix;
        this.logger = node.getLogger(ExecutorThreadFactory.class.getName());
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
        t.setContextClassLoader(classLoader);
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        if (true) {
            try {
                throw new RuntimeException("New Thread " + t);
            } catch (RuntimeException e1) {
                e1.printStackTrace();
                logger.log(Level.WARNING, "Hazelcast Created a New Thread", e1);
            }
        }
        return t;
    }
}