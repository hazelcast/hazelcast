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

package com.hazelcast.client.config;

import com.hazelcast.config.ConfigLoader;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.logging.Level;

/**
 * @mdogan 5/10/12
 */
public class ClientConfigBuilder {

    private static final ILogger logger = Logger.getLogger(ClientConfigBuilder.class.getName());

    public final static String GROUP_NAME = "hazelcast.client.group.name";
    public final static String GROUP_PASS = "hazelcast.client.group.pass";
    public final static String CONNECTION_TIMEOUT = "hazelcast.client.connection.timeout";
    public final static String CONNECTION_ATTEMPT_LIMIT = "hazelcast.client.connection.attempts.limit";
    public final static String RECONNECTION_TIMEOUT = "hazelcast.client.reconnection.timeout";
    public final static String ADDRESSES = "hazelcast.client.addresses";

    private final Properties props = new Properties();

    private final ClientConfig config = new ClientConfig();

    private String resource;

    public ClientConfigBuilder(String resource) throws IOException {
        super();
        URL url = ConfigLoader.locateConfig(resource);
        if (url == null) {
            throw new IllegalArgumentException("Could not load " + resource);
        }
        this.resource = resource;
        props.load(url.openStream());
    }

    public ClientConfigBuilder(File file) throws IOException {
        super();
        if (file == null) {
            throw new NullPointerException("File is null!");
        }
        this.resource = file.getAbsolutePath();
        props.load(new FileInputStream(file));
    }

    public ClientConfigBuilder(URL url) throws IOException {
        super();
        if (url == null) {
            throw new NullPointerException("URL is null!");
        }
        this.resource = url.toExternalForm();
        props.load(url.openStream());
    }

    public ClientConfigBuilder(InputStream in) throws IOException {
        super();
        if (in == null) {
            throw new NullPointerException("InputStream is null!");
        }
        props.load(in);
    }

    public ClientConfig build() {
        logger.log(Level.INFO, "Building ClientConfig " + (resource != null ? " using " + resource : "") + ".");
        if (props.containsKey(GROUP_NAME)) {
            config.getGroupConfig().setName(props.getProperty(GROUP_NAME));
        }
        if (props.containsKey(GROUP_PASS)) {
            config.getGroupConfig().setPassword(props.getProperty(GROUP_PASS));
        }
        if (props.containsKey(CONNECTION_TIMEOUT)) {
            config.setConnectionTimeout(Integer.parseInt(props.getProperty(CONNECTION_TIMEOUT)));
        }
        if (props.containsKey(CONNECTION_ATTEMPT_LIMIT)) {
            config.setConnectionAttemptLimit(Integer.parseInt(props.getProperty(CONNECTION_ATTEMPT_LIMIT)));
        }
        if (props.containsKey(RECONNECTION_TIMEOUT)) {
            config.setAttemptPeriod(Integer.parseInt(props.getProperty(RECONNECTION_TIMEOUT)));
        }
        if (props.containsKey(ADDRESSES)) {
            final String addressesProp = props.getProperty(ADDRESSES);
            if (addressesProp != null) {
                final String[] addresses = addressesProp.split("[,; ]");
                for (String address : addresses) {
                    address = address.trim();
                    if (address.length() > 0) {
                        config.addAddress(address);
                    }
                }
            }
        }
        return config;
    }

}
