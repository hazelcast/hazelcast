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
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexImpl;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.query.impl.predicates.AttributeUtils.readAttribute;

/**
 * Regex Predicate
 */
public class RegexPredicate implements Predicate, DataSerializable {
    private String attribute;
    private String regex;
    private volatile Pattern pattern;

    public RegexPredicate() {
    }

    public RegexPredicate(String attribute, String regex) {
        this.attribute = attribute;
        this.regex = regex;
    }

    @Override
    public boolean apply(Map.Entry entry) {
        Comparable attribute = readAttribute(entry, this.attribute);
        String firstVal = attribute == IndexImpl.NULL ? null : (String) attribute;
        if (firstVal == null) {
            return (regex == null);
        } else if (regex == null) {
            return false;
        } else {
            if (pattern == null) {
                pattern = Pattern.compile(regex);
            }
            Matcher m = pattern.matcher(firstVal);
            return m.matches();
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attribute);
        out.writeUTF(regex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        attribute = in.readUTF();
        regex = in.readUTF();
    }

    @Override
    public String toString() {
        return attribute + " REGEX '" + regex + "'";
    }
}
