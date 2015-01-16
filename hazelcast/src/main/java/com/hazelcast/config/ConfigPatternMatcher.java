package com.hazelcast.config;

/**
 * The ConfigPatternMatcher provides a strategy to match an item name to a configuration pattern.
 * <p/>
 * It is used on each Config.getXXXConfig() and ClientConfig.getXXXConfig() call for map, list, queue, set, executor, topic,
 * semaphore etc., so for example <code>itemName</code> is the name of a map and <code>configPatterns</code> are all defined
 * map configurations.
 * <p/>
 * If no configuration is found by the matcher it should return <tt>null</tt>. In this case the default config will be used
 * for this item then. If multiple configurations are found by the matcher a {@link com.hazelcast.config.ConfigurationException}
 * should be thrown.
 * <p/>
 * Since Hazelcast 3.5 the default matcher is {@link com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher}.
 */
public interface ConfigPatternMatcher {

    /**
     * Returns the best match for an item name out of a list of configuration patterns.
     *
     * @param configPatterns    list of configuration patterns
     * @param itemName          item name to match
     * @return a key of configPatterns which matches the item name or <tt>null</tt> if nothing matches
     * @throws ConfigurationException if ambiguous configurations are found
     */
    String matches(Iterable<String> configPatterns, String itemName) throws ConfigurationException;
}
