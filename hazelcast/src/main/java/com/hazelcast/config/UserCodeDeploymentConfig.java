/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.annotation.Beta;

/**
 * Configuration of User Code Deployment. When enabled it allows Hazelcast members to load classes from other cluster
 * members. This simplifies deployment as you do not have to deploy your domain classes into classpath of all
 * cluster members.
 */
@Beta
public class UserCodeDeploymentConfig {

    /**
     * Controls caching of caches loaded from remote members
     */
    public enum ClassCacheMode {
        /**
         * Never caches loaded classes. This is suitable for loading runnables, callables, entry processors, etc.
         */
        OFF,

        /**
         * Cache indefinitely. This is suitable when you load long-living objects, such as domain objects stored
         * in a map.
         */
        ETERNAL,
    }

    /**
     * Controls how to react on receiving a classloading request from a remote member
     */
    public enum ProviderMode {
        /**
         * Never serves classes to other members. This member will never server classes to remote members.
         */
        OFF,

        /**
         * Serve classes from local classpath only. Classes loaded from other members will be used locally,
         * but they won't be served to other members.
         */
        LOCAL_CLASSES_ONLY,

        /**
         * Serve classes loaded from both local classpath and from other members.
         */
        LOCAL_AND_CACHED_CLASSES
    }

    private ClassCacheMode classCacheMode = ClassCacheMode.ETERNAL;
    private ProviderMode providerMode = ProviderMode.LOCAL_AND_CACHED_CLASSES;
    private String blacklistedPrefixes;
    private String whitelistedPrefixes;
    private String providerFilter;
    private boolean enabled;

    /**
     * Enable or disable User Code Deployment. Default: {@code false}
     */
    public UserCodeDeploymentConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns true when User Code Deployment is enabled
     *
     * @return {@code true} when User Code Deployment is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Filter to constraint members to be used for classloading request when a user class
     * is not available locally.
     *
     * Filter format: {@code HAS_ATTRIBUTE:foo} this will send classloading requests
     * only to members which has a member attribute {@code foo} set. Value is ignored,
     * it can be any type. A present of the attribute is sufficient.
     *
     * This facility allows to have a fine grained control on classloading. You can e.g. start Hazelcast lite
     * members dedicated for class-serving.
     *
     * Example usage:
     * This member will load classes only from members with the {@code class-provider} attribute set.
     * It won't ask any other member to provide a locally unavailable class:
     * <code>
     * <pre>
     *     Config hazelcastConfig = new Config();
     *
     *     UserCodeDeploymentConfig userCodeDeploymentConfig = hazelcastConfig.getUserCodeDeploymentConfig();
     *     userCodeDeploymentConfig.setProviderFilter("HAS_ATTRIBUTE:class-provider");
     *
     *     HazecastInstance instance = Hazelcast.newHazelcastInstance(hazelcastConfig);
     * </pre>
     * </code>
     *
     * This member is marked with the <code>class-provider</code> attribute - the member configured above may him
     * it to provide a class which is not locally available:
     * <code>
     * <pre>
     * Config hazelcastConfig = new Config();
     *
     * MemberAttributeConfig memberAttributes = hazelcastConfig.getMemberAttributeConfig();
     * memberAttributes.setAttribute("class-provider", "true");
     *
     * HazecastInstance instance = Hazelcast.newHazelcastInstance(hazelcastConfig);
     * </pre>
     * </code>
     *
     * Setting the filter to null will allow to load classes from all members.
     * <p>
     * Default: {@code null}
     *
     * @return this instance of UserCodeDeploymentConfig for easy method-chaining
     * @see com.hazelcast.core.Member#setStringAttribute(String, String)
     */
    public UserCodeDeploymentConfig setProviderFilter(String providerFilter) {
        this.providerFilter = providerFilter;
        return this;
    }

    /**
     * Get current filter or {@code null} when no filter is defined.
     *
     * @return current filter or {@code null} when no filter is defined
     * @see #setProviderFilter(String)
     */
    public String getProviderFilter() {
        return providerFilter;
    }

    /**
     * Comma separated list of prefixes of classes which will never be loaded remotely.
     * A prefix can be a package name or a classname.
     * <p>
     * For example setting a blacklist prefix to {@code com.foo} will disable remote loading of all classes
     * from the {@code com.foo} package, but also classes from all sub-packages won't be loaded.
     * Eg. {@code com.foo.bar.MyClass} will be black-listed too.
     * <p>
     * When you set the blacklist to {@code com.foo.Class} then the Class will obviously be black-listed,
     * but a class {@code com.foo.ClassSuffix} will be blacklisted too.
     * <p>
     * Setting the prefixes to {@code null} will disable the blacklist.
     * <p>
     * Default: {@code null}
     *
     * @return this instance of UserCodeDeploymentConfig for easy method-chaining
     */
    public UserCodeDeploymentConfig setBlacklistedPrefixes(String blacklistedPrefixes) {
        this.blacklistedPrefixes = blacklistedPrefixes;
        return this;
    }

    /**
     * Return currently configured blacklist prefixes.
     *
     * @return currently configured blacklist prefixes
     * @see #setBlacklistedPrefixes(String)
     */
    public String getBlacklistedPrefixes() {
        return blacklistedPrefixes;
    }

    /**
     * Comma separated list of prefixes of classes which will be loaded remotely.
     * <p>
     * Use this to enable User Code Deployment of selected classes only and disable remote loading for all
     * other classes. This gives you a nice control over classloading.
     * <p>
     * The prefixes are interpreted by using the same rules as described at {@link #setBlacklistedPrefixes(String)}
     * <p>
     * Setting the prefix to {@code null} will disable the white-list and all non-blacklisted classes will be allowed
     * to load from remote members.
     *
     * @return this instance of UserCodeDeploymentConfig for easy method-chaining
     */
    public UserCodeDeploymentConfig setWhitelistedPrefixes(String whitelistedPrefixes) {
        this.whitelistedPrefixes = whitelistedPrefixes;
        return this;
    }

    /**
     * Return currently configured whitelist prefixes
     *
     * @return currently configured whitelist prefixes
     */
    public String getWhitelistedPrefixes() {
        return whitelistedPrefixes;
    }

    /**
     * Configure behaviour when providing classes to remote members.
     * <p>
     * Default: {@link ProviderMode#LOCAL_AND_CACHED_CLASSES}
     *
     * @return this instance of UserCodeDeploymentConfig for easy method-chaining
     * @see ProviderMode
     */
    public UserCodeDeploymentConfig setProviderMode(ProviderMode providerMode) {
        this.providerMode = providerMode;
        return this;
    }

    /**
     * Return the current ProviderMode
     *
     * @return current ProviderMode
     */
    public ProviderMode getProviderMode() {
        return providerMode;
    }

    /**
     * Configure caching of classes loaded from remote members.
     * <p>
     * Default: {@link ClassCacheMode#ETERNAL}
     *
     * @return this instance of UserCodeDeploymentConfig for easy method-chaining
     * @see ClassCacheMode
     */
    public UserCodeDeploymentConfig setClassCacheMode(ClassCacheMode classCacheMode) {
        this.classCacheMode = classCacheMode;
        return this;
    }

    /**
     * Return the current ClassCacheMode
     *
     * @return current ProviderMode
     */
    public ClassCacheMode getClassCacheMode() {
        return classCacheMode;
    }
}
