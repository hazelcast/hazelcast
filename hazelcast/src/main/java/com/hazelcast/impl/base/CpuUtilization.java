/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl.base;

import com.hazelcast.util.Clock;
import com.hazelcast.util.ThreadStats;

public class CpuUtilization {

    public volatile ThreadStats serviceThread = new ThreadStats(0, 0, 0, true);
    public volatile ThreadStats inThread = new ThreadStats(0, 0, 0, true);
    public volatile ThreadStats outThread = new ThreadStats(0, 0, 0, true);

    @Override
    public String toString() {
        long now = Clock.currentTimeMillis();
        return "CpuUtilization {" +
                "\n\tserviceThread =" + serviceThread + " lastExe:" + serviceThread.getSecondsBetween(now) +
                "\n\tinThread      =" + inThread + " lastExe:" + inThread.getSecondsBetween(now) +
                "\n\toutThread     =" + outThread + " lastExe:" + outThread.getSecondsBetween(now) +
                "\n}";
    }
}
