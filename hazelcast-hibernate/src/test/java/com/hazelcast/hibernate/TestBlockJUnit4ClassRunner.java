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

package com.hazelcast.hibernate;

import com.hazelcast.impl.GroupProperties;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.io.InputStream;
import java.util.logging.LogManager;

/**
 * @mdogan 7/19/12
 */
public class TestBlockJUnit4ClassRunner extends BlockJUnit4ClassRunner {

    static {
        try {
            InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("logging.properties");
            LogManager.getLogManager().readConfiguration(in);
            in.close();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
    }

    /**
     * Creates a BlockJUnit4ClassRunner to run {@code klass}
     *
     * @throws org.junit.runners.model.InitializationError
     *          if the test class is malformed.
     */
    public TestBlockJUnit4ClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }
}
