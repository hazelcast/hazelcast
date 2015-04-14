/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.config;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.SpiJoinerFactory;

import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * service class to load the joiner via spi.
 */
public class SpiJoinerLoadService {

    private static final ILogger LOGGER = Logger.getLogger(SpiJoinerLoadService.class);

    private static SpiJoinerLoadService service;
    private ServiceLoader<SpiJoinerFactory> loader;

    private SpiJoinerLoadService() {
        loader = ServiceLoader.load(SpiJoinerFactory.class);
    }

    public static synchronized SpiJoinerLoadService getInstance() {
        if (service == null) {
            service = new SpiJoinerLoadService();
        }
        return service;
    }

    /**
     * get the joiner service with the given type.
     * @param type
     */
    public SpiJoinerFactory getJoiner(String type) {
        SpiJoinerFactory result = null;

        try {
            Iterator<SpiJoinerFactory> joiners = loader.iterator();
            while (result == null && joiners.hasNext()) {
                SpiJoinerFactory joiner = joiners.next();
                if(type.equals(joiner.getType())) {
                    result = joiner;
                }
            }
        } catch (ServiceConfigurationError serviceError) {
            result = null;
            LOGGER.severe("Error while getting joiner service with type" + type, serviceError);
        }
        return result;
    }

}
