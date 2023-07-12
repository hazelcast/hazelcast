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

package com.hazelcast.internal.tpcengine;

import java.util.concurrent.atomic.AtomicLong;

public class PrintAtomicLongThread extends Thread {

    private final String prefix;
    private volatile boolean stopped = false;
    private final AtomicLong atomicLong;

    public PrintAtomicLongThread(String prefix, AtomicLong atomicLong) {
        super("PrintAtomicLongThread");
        this.atomicLong = atomicLong;
        this.prefix = prefix;
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }

            System.out.println(prefix + atomicLong);
        }
    }

    public void shutdown() {
        stopped = true;
        interrupt();
    }
}
