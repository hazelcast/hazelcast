/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.monitor.LocalCountDownLatchStats;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CountDownLatchClientProxy implements ICountDownLatch {
    private final String name;
    private final ProxyHelper proxyHelper;

    public CountDownLatchClientProxy(HazelcastClient hazelcastClient, String name) {
        this.name = name;
        this.proxyHelper = new ProxyHelper(hazelcastClient.getSerializationService(), hazelcastClient.getConnectionPool());
    }

    public void await() throws MemberLeftException, InterruptedException {
        await(Long.MAX_VALUE, MILLISECONDS);
    }

    public boolean await(long timeout, TimeUnit unit) throws MemberLeftException, InterruptedException {
        String[] args = new String[]{getName(), String.valueOf(unit.toMillis(timeout))};
        Protocol response = proxyHelper.doCommand(null, Command.CDLAWAIT, args, null);
        if ("member.left.exception".equalsIgnoreCase(response.args[0])) {
            String hostname = response.args[1];
            String port = response.args[2];
            Member member = null;
            try {
                member = new MemberImpl(new Address(hostname, Integer.valueOf(port)), false);
            } catch (UnknownHostException e) {
            }
            throw new MemberLeftException(member);
        } else {
            return Boolean.valueOf(response.args[0]);
        }
    }

    public void countDown() {
        proxyHelper.doCommand(null, Command.CDLCOUNTDOWN, getName(), null);
    }

    public int getCount() {
        return proxyHelper.doCommandAsInt(null, Command.CDLGETCOUNT, new String[]{getName()}, null);
    }

    public boolean trySetCount(int count) {
        String[] args = new String[]{getName(), String.valueOf(count)};
        return proxyHelper.doCommandAsBoolean(null, Command.CDLSETCOUNT, args, null);
    }

    public void destroy() {
        proxyHelper.destroy();
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    public LocalCountDownLatchStats getLocalCountDownLatchStats() {
        throw new UnsupportedOperationException();
    }
}
