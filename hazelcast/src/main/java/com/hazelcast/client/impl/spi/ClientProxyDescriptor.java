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

package com.hazelcast.client.impl.spi;

/**
 * Descriptor of Hazelcast client proxies
 *
 * Provided by corresponding provider (see ClientProxyDescriptorProvider).
 * Contains information about client proxy service.
 *
 * Examples:
 * <pre>
 *           JetClientProxyDescriptor
 *           TreeClientProxyDescriptor
 *           ....
 * </pre>
 */
public interface ClientProxyDescriptor {
    /**
     * @return - name of the corresponding client service
     */
    String getServiceName();

    /**
     * @return - class of the corresponding proxy class
     */
    Class<? extends ClientProxy> getClientProxyClass();
}
