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

package com.hazelcast.monitor.server;

import org.junit.Test;

import static com.hazelcast.monitor.client.MapStatisticsPanel.formatMemorySize;

public class HazelcastServerImplTest {

    @Test
    public void run() {
        System.out.println(toPrecision((double)2000/1024));
        System.out.println("1234: " + formatMemorySize(1234));
        System.out.println("1234345: " + formatMemorySize(1234345));
        System.out.println("123: " + formatMemorySize(123));
        System.out.println("123434: " + formatMemorySize(123434));
        System.out.println("1234999999: " + formatMemorySize(1234999999));
        System.out.println("0: " + formatMemorySize(0));
        System.out.println("1024: " + formatMemorySize(1024));
    }

    private static String toPrecision(double dbl){
        int ix = (int)(dbl * 100.0); // scale it
        double dbl2 = ((double)ix)/100.0;
        return String.valueOf(dbl2);
    }
}
