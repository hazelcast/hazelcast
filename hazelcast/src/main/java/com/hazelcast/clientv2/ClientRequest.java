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

package com.hazelcast.clientv2;

import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Portable;

/**
 * @mdogan 2/20/13
 */
public interface ClientRequest extends Portable {

    Object process() throws Exception;

    String getServiceName();

    void setClientEngine(ClientEngine clientEngine);

    void setService(Object service);

    void setConnection(Connection connection);

}
