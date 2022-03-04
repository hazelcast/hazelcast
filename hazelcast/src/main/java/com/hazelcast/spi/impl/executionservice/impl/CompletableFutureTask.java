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

package com.hazelcast.spi.impl.executionservice.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class CompletableFutureTask implements Runnable {
    private final List<CompletableFutureEntry> entries = new ArrayList<CompletableFutureEntry>();
    private final Lock entriesLock = new ReentrantLock();

    <V> void registerCompletableFutureEntry(CompletableFutureEntry<V> entry) {
        entriesLock.lock();
        try {
            entries.add(entry);
        } finally {
            entriesLock.unlock();
        }
    }

    @Override
    public void run() {
        List<CompletableFutureEntry> removableEntries = removableEntries();
        removeEntries(removableEntries);
    }

    private void removeEntries(List<CompletableFutureEntry> removableEntries) {
        if (removableEntries.isEmpty()) {
            return;
        }

        entriesLock.lock();
        try {
            entries.removeAll(removableEntries);
        } finally {
            entriesLock.unlock();
        }
    }

    private List<CompletableFutureEntry> removableEntries() {
        CompletableFutureEntry[] entries = copyEntries();

        List<CompletableFutureEntry> removableEntries = Collections.EMPTY_LIST;
        for (CompletableFutureEntry entry : entries) {
            if (entry.processState()) {
                if (removableEntries.isEmpty()) {
                    removableEntries = new ArrayList<CompletableFutureEntry>(entries.length / 2);
                }

                removableEntries.add(entry);
            }
        }
        return removableEntries;
    }

    private CompletableFutureEntry[] copyEntries() {
        if (entries.isEmpty()) {
            return new CompletableFutureEntry[]{};
        }

        CompletableFutureEntry[] copy;
        entriesLock.lock();
        try {
            copy = new CompletableFutureEntry[entries.size()];
            copy = entries.toArray(copy);
        } finally {
            entriesLock.unlock();
        }
        return copy;
    }

    @Override
    public String toString() {
        return "CompletableFutureTask{}";
    }
}
