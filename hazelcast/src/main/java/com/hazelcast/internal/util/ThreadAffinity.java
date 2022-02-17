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

package com.hazelcast.internal.util;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.ThreadAffinityHelper.isAffinityAvailable;

/**
 * Contains the thread affinity logic for certain threads.
 *
 * Inside is a list of CPU bitmaps and using the {@link #nextAllowedCpus()} there is a round robin
 * over the list of the CPU bitmaps. The reason for a round robin is that the same ThreadAffinity can
 * be used when threads get stopped and new threads created.
 *
 * This class is threadsafe.
 */
public class ThreadAffinity {
    public static final ThreadAffinity DISABLED = new ThreadAffinity(null);

    final List<BitSet> allowedCpusList;
    final AtomicInteger threadIndex = new AtomicInteger();

    public ThreadAffinity(String affinity) {
        allowedCpusList = parse(affinity);

        if (allowedCpusList.isEmpty()) {
            return;
        }

        if (!isAffinityAvailable()) {
            throw new RuntimeException("Can't use affinity '" + affinity + "'. Thread affinity support is not available.");
        }
    }

    /**
     * Creates a new ThreadAffinity based on a system property.
     *
     * If no property is set, then affinity is disabled.
     *
     * @param property the name of the system property.
     * @return the created ThreadAffinity.
     * @throws InvalidAffinitySyntaxException if there is a problem with the value.
     */
    public static ThreadAffinity newSystemThreadAffinity(String property) {
        String value = System.getProperty(property);
        try {
            return new ThreadAffinity(value);
        } catch (InvalidAffinitySyntaxException e) {
            throw new InvalidAffinitySyntaxException("Invalid affinity syntax for System property '" + property + "'."
                    + " Value '" + value + "'. "
                    + " Errormessage '" + e.getMessage() + "'");
        }
    }

    static List<BitSet> parse(String affinity) {
        List<BitSet> cpus = new ArrayList<>();
        if (affinity == null) {
            return cpus;
        }

        affinity = affinity.trim();
        if (affinity.isEmpty()) {
            return cpus;
        }

        List<CpuGroup> groups = new AffinityParser(affinity).parse();
        for (CpuGroup group : groups) {
            BitSet allowedCpus = new BitSet();

            for (Integer cpu : group.cpus) {
                allowedCpus.set(cpu);
            }
            for (int k = 0; k < group.threadCount; k++) {
                cpus.add(allowedCpus);
            }
        }

        return cpus;
    }

    public int getThreadCount() {
        return allowedCpusList.size();
    }

    public BitSet nextAllowedCpus() {
        if (allowedCpusList.isEmpty()) {
            return null;
        }

        int index = threadIndex.getAndIncrement() % allowedCpusList.size();
        return allowedCpusList.get(index);
    }

    public boolean isEnabled() {
        return !allowedCpusList.isEmpty();
    }

    static class AffinityParser {
        private final String string;
        private final List<CpuGroup> groups = new ArrayList<>();
        private int index;
        private int digit;
        private int integer;
        private int fromRange;
        private int toRange;

        AffinityParser(String string) {
            this.string = string;
        }

        List<CpuGroup> parse() {
            if (!expression() || index < string.length()) {
                throw new InvalidAffinitySyntaxException("Syntax Error at " + index);
            }

            // verification that we have no duplicate cpus.
            BitSet usedCpus = new BitSet();
            for (CpuGroup group : groups) {
                for (Integer cpu : group.cpus) {
                    if (usedCpus.get(cpu)) {
                        throw new InvalidAffinitySyntaxException("Duplicate CPU found, offending CPU=" + cpu);
                    }
                    usedCpus.set(cpu);
                }
            }

            return groups;
        }

        boolean expression() {
            if (!item()) {
                return false;
            }

            while (character(',')) {
                if (!item()) {
                    return false;
                }
            }

            return true;
        }

        boolean item() {
            if (range()) {
                for (int cpu = fromRange; cpu <= toRange; cpu++) {
                    CpuGroup group = new CpuGroup();
                    group.cpus.add(cpu);
                    group.threadCount = 1;
                    groups.add(group);
                }
                return true;
            } else {
                return group();
            }
        }

        boolean range() {
            if (!integer()) {
                return false;
            }
            fromRange = integer;
            toRange = integer;
            if (character('-')) {
                if (!integer()) {
                    return false;
                }
                toRange = integer;
                if (toRange < fromRange) {
                    error("ToRange can't smaller than fromRange, toRange=" + toRange + " fromRange=" + fromRange + ".");
                }
            }

            return true;
        }

        private void error(String error) {
            throw new InvalidAffinitySyntaxException(error + " at index:" + index);
        }

        @SuppressWarnings("checkstyle:NPathComplexity")
        boolean group() {
            if (!character('[')) {
                return false;
            }

            if (!range()) {
                return false;
            }

            CpuGroup group = new CpuGroup();
            addCpuRangeToGroup(group);

            while (character(',')) {
                if (!range()) {
                    return false;
                }

                addCpuRangeToGroup(group);
            }

            if (!character(']')) {
                return false;
            }

            if (character(':')) {
                if (!integer()) {
                    return false;
                }
                group.threadCount = integer;
                if (group.threadCount == 0) {
                    error("Thread count can't be 0.");
                } else if (group.threadCount > group.cpus.size()) {
                    error("Thread count can't be larger than number of cpu's in the group. "
                            + "Thread count = " + group.threadCount + " cpus:" + group.cpus);
                }
            } else {
                group.threadCount = group.cpus.size();
            }

            groups.add(group);
            return true;
        }

        private void addCpuRangeToGroup(CpuGroup group) {
            for (int k = fromRange; k <= toRange; k++) {
                group.cpus.add(k);
            }
        }

        @SuppressWarnings("checkstyle:magicnumber")
        boolean integer() {
            if (!digit()) {
                return false;
            }

            integer = digit;
            for (; ; ) {
                if (!digit()) {
                    return true;
                }
                integer = integer * 10;
                integer += digit;
            }
        }

        boolean digit() {
            if (index >= string.length()) {
                return false;
            }

            char c = string.charAt(index);
            if (Character.isDigit(c)) {
                index++;
                digit = Character.getNumericValue(c);
                return true;
            } else {
                return false;
            }
        }

        boolean character(char expected) {
            if (index >= string.length()) {
                return false;
            }

            char c = string.charAt(index);
            if (c == expected) {
                index++;
                return true;
            } else {
                return false;
            }
        }
    }

    static class InvalidAffinitySyntaxException extends RuntimeException {
        InvalidAffinitySyntaxException(String message) {
            super(message);
        }
    }

    static class CpuGroup {
        List<Integer> cpus = new LinkedList<>();
        int threadCount = -1;

        @Override
        public String toString() {
            return "CpuGroup{"
                    + "cpus=" + cpus
                    + ", count=" + threadCount
                    + '}';
        }
    }
}
