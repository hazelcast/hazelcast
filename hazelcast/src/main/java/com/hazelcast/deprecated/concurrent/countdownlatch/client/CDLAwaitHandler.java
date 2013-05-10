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

package com.hazelcast.deprecated.concurrent.countdownlatch.client;

import com.hazelcast.deprecated.client.ClientCommandHandler;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchProxy;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.deprecated.nio.Protocol;

import java.util.concurrent.TimeUnit;

public class CDLAwaitHandler extends ClientCommandHandler {
    final CountDownLatchService countDownLatchService;

    public CDLAwaitHandler(CountDownLatchService countDownLatchService) {
        super();
        this.countDownLatchService = countDownLatchService;
    }

    @Override
    public Protocol processCall(Node node, Protocol protocol) {
        String name = protocol.args[0];
        long time = Long.valueOf(protocol.args[1]);
        CountDownLatchProxy cdlp = countDownLatchService.createDistributedObjectForClient(name);
        try {
            boolean result = cdlp.await(time, TimeUnit.MILLISECONDS);
            return protocol.success(String.valueOf(result));
        } catch (MemberLeftException e) {
            MemberImpl m = (MemberImpl)e.getMember();
            String[] args = new String[3];
            args[0] = "member.left.exception";
            args[1] = m.getAddress().getHost() ;
            args[2] = String.valueOf(m.getAddress().getPort());
            return protocol.success(args);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
