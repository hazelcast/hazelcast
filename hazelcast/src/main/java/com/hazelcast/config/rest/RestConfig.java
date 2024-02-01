/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.rest;

import com.hazelcast.spi.annotation.Beta;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * This class allows controlling the Hazelcast REST API feature.
 *
 * @since 5.4
 */
@Beta
public class RestConfig {

    private static final int DEFAULT_PORT = 8443;
    private static final int DEFAULT_DURATION_MINUTES = 15;
    private static final Duration DEFAULT_DURATION = Duration.of(DEFAULT_DURATION_MINUTES, ChronoUnit.MINUTES);

    /**
     * Indicates whether the RestConfig is enabled.
     */
    private boolean enabled = true;

    /**
     * The port number for the Rest API server endpoint.
     */
    private int port = DEFAULT_PORT;

    /**
     * The name of the Rest security realm which should be already configured.
     */
    private String securityRealm;

    /**
     * Duration for a token to remain valid.
     */
    private Duration tokenValidityDuration = DEFAULT_DURATION;

    /**
     * Default constructor for RestConfig.
     */
    public RestConfig() {
    }

    /**
     * Checks if the RestConfig is enabled.
     *
     * @return true if the RestConfig is enabled, false otherwise.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the enabled status of the RestConfig.
     *
     * @param enabled the new enabled status.
     * @return the updated RestConfig.
     */
    public RestConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Gets the port of the RestConfig.
     *
     * @return the port of the RestConfig.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port of the RestConfig.
     *
     * @param port the new port.
     * @return the updated RestConfig.
     */
    public RestConfig setPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * Gets the name of the Rest security realm.
     *
     * @return the name of the realm.
     */
    public String getSecurityRealm() {
        return securityRealm;
    }

    /**
     * Sets the name of the Rest security realm.
     *
     * @param securityRealm the name of the realm. This should be an already defined valid security realm.
     */
    public void setSecurityRealm(String securityRealm) {
        this.securityRealm = securityRealm;
    }

    /**
     * Gets the token validity duration.
     *
     * @return the duration for which the token is valid.
     */
    public Duration getTokenValidityDuration() {
        return tokenValidityDuration;
    }

    /**
     * Sets the expiration duration for jwt token.
     *
     * @param tokenValidityDuration the duration for which the token should be valid.
     * @warning This resolution for tokenValidityDuration can not be more than a second.
     */
    public void setTokenValidityDuration(Duration tokenValidityDuration) {
        this.tokenValidityDuration = tokenValidityDuration;
    }

    /**
     * Returns a string representation of the RestConfig.
     *
     * @return a string representation of the RestConfig.
     */
    @Override
    public String toString() {
        return "RestConfig{enabled=" + enabled + ", port=" + port + ", securityRealm='" + securityRealm + '\''
                + ", tokenValidityDuration=" + tokenValidityDuration + '}';
    }
}
