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

package com.hazelcast.spi.impl.servicemanager;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Consumer;

/**
 * Responsible for managing services.
 */
public interface ServiceManager {

    /**
     * Gets the ServiceInfo for a service by serviceName.
     *
     * @param serviceName the name of the service.
     * @return the found ServiceInfo or null if nothing is found.
     */
    ServiceInfo getServiceInfo(@Nonnull String serviceName);

    /**
     * Gets all the service info's for services that implement a given class/interface.
     *
     * @param serviceClass the class/interface the service should implement.
     * @return a List of the found ServiceInfo. List will be empty if nothing is found.
     */
    List<ServiceInfo> getServiceInfos(Class serviceClass);

    /**
     * Gets a Service by serviceName.
     *
     * @param serviceName the name of the service.
     * @param <T>
     * @return the found service or null if nothing is found.
     */
    <T> T getService(@Nonnull String serviceName);

    /**
     * Gets all services implementing a certain class/interface.
     *
     * <b>CoreServices will be placed at the beginning of the list.</b>
     *
     * @param serviceClass the class/interface to check for.
     * @param <S>
     * @return the found services.
     */
    <S> List<S> getServices(Class<S> serviceClass);

    /**
     * @param serviceClass the class/interface the service should implement.
     * @param consumer     to consume {@link ServiceInfo}
     */
    void forEachMatchingService(Class serviceClass, Consumer<ServiceInfo> consumer);
}
