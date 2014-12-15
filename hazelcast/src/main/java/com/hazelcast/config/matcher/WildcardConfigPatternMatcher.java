package com.hazelcast.config.matcher;

import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.ConfigurationException;

/**
 * This <code>ConfigPatternMatcher</code> supports a simplified wildcard matching.
 * See "Config.md ## Using Wildcard" for details about the syntax options.
 * <p/>
 * Throws {@link com.hazelcast.config.ConfigurationException} is multiple configurations are found.
 */
public class WildcardConfigPatternMatcher implements ConfigPatternMatcher {

    @Override
    public String matches(Iterable<String> configPatterns, String itemName) throws ConfigurationException {
        String candidate = null;
        for (String pattern : configPatterns) {
            if (matches(pattern, itemName)) {
                if (candidate != null) {
                    throw new ConfigurationException(itemName, candidate, pattern);
                }
                candidate = pattern;
            }
        }
        return candidate;
    }

    /**
     * This method is public to be accessible by {@link com.hazelcast.security.permission.InstancePermission}.
     *
     * @param pattern     configuration pattern to match with
     * @param itemName    item name to match
     * @return <tt>true</tt> if itemName matches, <tt>false</tt> otherwise
     */
    public boolean matches(String pattern, String itemName) {
        final int index = pattern.indexOf('*');
        if (index == -1) {
            return itemName.equals(pattern);
        }

        final String firstPart = pattern.substring(0, index);
        if (!itemName.startsWith(firstPart)) {
            return false;
        }

        final String secondPart = pattern.substring(index + 1);
        if (!itemName.endsWith(secondPart)) {
            return false;
        }

        return true;
    }
}
