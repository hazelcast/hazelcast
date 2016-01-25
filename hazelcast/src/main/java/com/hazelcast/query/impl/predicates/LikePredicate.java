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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Like Predicate
 */
public class LikePredicate extends AbstractPredicate {

    protected String expression;
    private volatile Pattern pattern;

    public LikePredicate() {
    }

    public LikePredicate(String attributeName, String expression) {
        this.attributeName = attributeName;
        this.expression = expression;
    }

    @Override
    protected boolean applyForSingleAttributeValue(Map.Entry mapEntry, Comparable attributeValue) {
        String attributeValueString = (String) attributeValue;
        if (attributeValueString == null) {
            return (expression == null);
        } else if (expression == null) {
            return false;
        } else {
            if (pattern == null) {
                // we quote the input string then escape then replace % and _
                // at the end we have a regex pattern look like : \QSOME_STRING\E.*\QSOME_OTHER_STRING\E
                final String quotedExpression = Pattern.quote(expression);
                String regex = quotedExpression
                        //escaped %
                        .replaceAll("(?<!\\\\)[%]", "\\\\E.*\\\\Q")
                                //escaped _
                        .replaceAll("(?<!\\\\)[_]", "\\\\E.\\\\Q")
                                //non escaped %
                        .replaceAll("\\\\%", "%")
                                //non escaped _
                        .replaceAll("\\\\_", "_");
                int flags = getFlags();
                pattern = Pattern.compile(regex, flags);
            }
            Matcher m = pattern.matcher(attributeValueString);
            return m.matches();
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attributeName);
        out.writeUTF(expression);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        attributeName = in.readUTF();
        expression = in.readUTF();
    }


    protected int getFlags() {
        //no addFlag
        return 0;
    }

    @Override
    public String toString() {
        return attributeName + " LIKE '" + expression + "'";
    }

    @Override
    public int getId() {
        return PredicateDataSerializerHook.LIKE_PREDICATE;
    }
}
