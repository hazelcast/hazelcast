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

package com.hazelcast.topic.proxy;

import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.topic.PublishOperation;
import com.hazelcast.topic.TopicService;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

/**
 * User: sancar
 * Date: 12/31/12
 * Time: 12:08 PM
 */
public class TotalOrderedTopicProxy extends TopicProxy {

    public TotalOrderedTopicProxy(String name, NodeEngine nodeEngine, TopicService service) {
        super(name, nodeEngine, service);
    }

    @Override
    public void publish(Object message) {
        try {
            final NodeEngine nodeEngine = getNodeEngine();
            PublishOperation operation = new PublishOperation(getName(), nodeEngine.toData(message));
            final int partitionId = nodeEngine.getPartitionService().getPartitionId(getName());
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(TopicService.SERVICE_NAME, operation, partitionId).build();
            Future f = inv.invoke();
            f.get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }


}
