/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.AttributePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.ValidationUtil;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Like Predicate
 */
public class LikePredicate implements AttributePredicate, DataSerializable {
    protected String attribute;
    protected String second;
    private volatile Pattern pattern;

    public LikePredicate() {
    }

    public LikePredicate(String attribute, String second) {
        this.attribute = attribute;
        this.second = second;
    }

    @Override
    public boolean apply(Map.Entry entry) {
        Comparable attribute = readAttribute(entry, this.attribute);
        String firstVal = attribute == IndexImpl.NULL ? null : (String) attribute;
        if (firstVal == null) {
            return (second == null);
        } else if (second == null) {
            return false;
        } else {
            if (pattern == null) {
                // we quote the input string then escape then replace % and _
                // at the end we have a regex pattern look like : \QSOME_STRING\E.*\QSOME_OTHER_STRING\E
                final String quoted = Pattern.quote(second);
                String regex = quoted
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
            Matcher m = pattern.matcher(firstVal);
            return m.matches();
        }
    }

    @Override
    public String getAttribute() {
        return attribute;
    }

    @Override
    public boolean isSubSet(Predicate predicate) {
        // TODO: a sophisticated comparison algorithm for like queries
        return equals(predicate);
    }

    @Override
    public boolean equals(Object predicate) {
        if (predicate instanceof LikePredicate) {
            LikePredicate p = (LikePredicate) predicate;
            return ValidationUtil.equalOrNull(p.attribute, attribute) && ValidationUtil.equalOrNull(p.second, second);
        }
        return false;
    }

    public int hashCode() {
        int result = attribute.hashCode();
        result = 31 * result + (second != null ? second.hashCode() : 0);
        return result;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attribute);
        out.writeUTF(second);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        attribute = in.readUTF();
        second = in.readUTF();
    }

    private Comparable readAttribute(Map.Entry entry, String attribute) {
        QueryableEntry queryableEntry = (QueryableEntry) entry;
        Comparable value = queryableEntry.getAttribute(attribute);
        if (value == null) {
            return IndexImpl.NULL;
        }
        return value;
    }

    protected int getFlags() {
        //no flags
        return 0;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(attribute)
                .append(" LIKE '")
                .append(second)
                .append("'");
        return builder.toString();
    }
}
