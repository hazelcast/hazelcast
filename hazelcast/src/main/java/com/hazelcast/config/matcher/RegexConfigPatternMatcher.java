package com.hazelcast.config.matcher;

import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.ConfigurationException;

import java.util.regex.Pattern;

/**
 * This <code>ConfigPatternMatcher</code> uses Java regular expressions for matching.
 * <p/>
 * Throws {@link com.hazelcast.config.ConfigurationException} is multiple configurations are found.
 */
public class RegexConfigPatternMatcher implements ConfigPatternMatcher {

    private final int flags;

    public RegexConfigPatternMatcher() {
        this(0);
    }

    public RegexConfigPatternMatcher(int flags) {
        this.flags = flags;
    }

    @Override
    public String matches(Iterable<String> configPatterns, String itemName) throws ConfigurationException {
        String candidate = null;
        for (String pattern : configPatterns) {
            if (Pattern.compile(pattern, flags).matcher(itemName).find()) {
                if (candidate != null) {
                    throw new ConfigurationException(itemName, candidate, pattern);
                }
                candidate = pattern;
            }
        }
        return candidate;
    }
}
