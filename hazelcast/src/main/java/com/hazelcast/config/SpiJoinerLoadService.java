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
import com.hazelcast.spi.SpiJoiner;

import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * service class to load the joiner via spi.
 */
public class SpiJoinerLoadService {

    private static final ILogger LOGGER = Logger.getLogger(SpiJoinerLoadService.class);

    private static SpiJoinerLoadService service;
    private ServiceLoader<SpiJoiner> loader;

    private SpiJoinerLoadService() {
        loader = ServiceLoader.load(SpiJoiner.class);
    }

    public static synchronized SpiJoinerLoadService getInstance() {
        if (service == null) {
            service = new SpiJoinerLoadService();
        }
        return service;
    }

    /**
     * get the joiner service with the given tag name.
     * @param tagName
     */
    public SpiJoiner getJoiner(String tagName) {
        SpiJoiner result = null;

        try {
            Iterator<SpiJoiner> joiners = loader.iterator();
            while (result == null && joiners.hasNext()) {
                SpiJoiner joiner = joiners.next();
                if(tagName.equals(joiner.getTagName())) {
                    result = joiner;
                }
            }
        } catch (ServiceConfigurationError serviceError) {
            result = null;
            LOGGER.severe("Error while getting joiner service with tagName" + tagName, serviceError);
        }
        return result;
    }

}
