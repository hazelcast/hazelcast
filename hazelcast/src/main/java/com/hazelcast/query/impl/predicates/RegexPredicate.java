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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.BinaryInterface;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Regex Predicate
 */
@BinaryInterface
public class RegexPredicate extends AbstractPredicate {

    private String regex;
    private volatile Pattern pattern;

    public RegexPredicate() {
    }

    public RegexPredicate(String attributeName, String regex) {
        super(attributeName);
        this.regex = regex;
    }

    @Override
    protected boolean applyForSingleAttributeValue(Map.Entry mapEntry, Comparable attributeValue) {
        String stringAttributeValue = (String) attributeValue;
        if (stringAttributeValue == null) {
            return (regex == null);
        } else if (regex == null) {
            return false;
        } else {
            if (pattern == null) {
                pattern = Pattern.compile(regex);
            }
            Matcher m = pattern.matcher(stringAttributeValue);
            return m.matches();
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeUTF(regex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        regex = in.readUTF();
    }

    @Override
    public String toString() {
        return attributeName + " REGEX '" + regex + "'";
    }

    @Override
    public int getId() {
        return PredicateDataSerializerHook.REGEX_PREDICATE;
    }
}
