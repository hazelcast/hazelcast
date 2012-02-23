/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.util;

public class CounterService {
    public static final Counter userCounter = new Counter("user");
    public static final Counter serviceCounter = new Counter("serviceThread");

    public static class Counter {
        final String name;
        long totalCount;
        long totalElapsedTime;

        public Counter(String name) {
            this.name = name;
        }

        public void add(long elapsedTime) {
            totalCount++;
            totalElapsedTime += (elapsedTime / 1000);
        }

        @Override
        public String toString() {
            double ave = ((double) totalElapsedTime) / totalCount;
            return "Counter{" +
                    "name='" + name + '\'' +
                    ", totalCount=" + totalCount +
                    ", totalElapsedTime=" + totalElapsedTime +
                    ", ave=" + ave +
                    '}';
        }
    }

    public static void print() {
        System.out.println(userCounter);
        System.out.println("=======================");
        System.out.println(serviceCounter);
        System.out.println("=======================");
    }
}
