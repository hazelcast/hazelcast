/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.BinaryInterface;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Regex Predicate
 */
@BinaryInterface
public class RegexPredicate extends AbstractPredicate {

    private static final long serialVersionUID = 1L;

    private String regex;
    private transient volatile Pattern pattern;

    public RegexPredicate() {
    }

    public RegexPredicate(String attributeName, String regex) {
        super(attributeName);
        this.regex = regex;
    }

    @Override
    protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
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
        out.writeString(regex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        regex = in.readString();
    }

    @Override
    public String toString() {
        return attributeName + " REGEX '" + regex + "'";
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.REGEX_PREDICATE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        if (!(o instanceof RegexPredicate)) {
            return false;
        }

        RegexPredicate that = (RegexPredicate) o;
        if (!that.canEqual(this)) {
            return false;
        }

        return regex != null ? regex.equals(that.regex) : that.regex == null;
    }

    @Override
    public boolean canEqual(Object other) {
        return (other instanceof RegexPredicate);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (regex != null ? regex.hashCode() : 0);
        return result;
    }
}
