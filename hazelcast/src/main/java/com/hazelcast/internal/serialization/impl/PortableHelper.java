package com.hazelcast.internal.serialization.impl;


import javax.annotation.Nullable;
import java.util.regex.Pattern;

public class PortableHelper {

    private static final String NO_SQUARE_BRACKETS_EXP = "[^\\Q[]\\E]";
    private static final String SQUARE_BRACKETS_EXP = "\\[([^\\Q[]\\E])*\\]";
    private static final Pattern COLLECTION_ARGS_PATTERN = Pattern.compile(
            String.format("^((%s)+(%s))+$", NO_SQUARE_BRACKETS_EXP, SQUARE_BRACKETS_EXP));

    @Nullable
    static String extractArgumentsFromAttributeName(String attributeNameWithArguments) {
        int start = attributeNameWithArguments.lastIndexOf('[');
        int end = attributeNameWithArguments.lastIndexOf(']');
        if (COLLECTION_ARGS_PATTERN.matcher(attributeNameWithArguments).matches()) {
            return attributeNameWithArguments.substring(start + 1, end);
        } else if (start < 0 && end < 0) {
            return null;
        }
        throw new IllegalArgumentException("Wrong argument input passed to extractor " + attributeNameWithArguments);
    }

    static String extractAttributeNameNameWithoutArguments(String attributeNameWithArguments) {
        if (COLLECTION_ARGS_PATTERN.matcher(attributeNameWithArguments).matches()) {
            int start = attributeNameWithArguments.lastIndexOf('[');
            return attributeNameWithArguments.substring(0, start);
        } else {
            return attributeNameWithArguments;
        }
    }


}
