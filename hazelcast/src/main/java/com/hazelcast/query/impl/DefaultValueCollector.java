/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.impl.getters.MultiResult;

import java.util.ArrayList;
import java.util.List;

public class DefaultValueCollector extends ValueCollector {

    private Object value;
    private List<Object> values;

    private byte[] bytes;
    private short[] shorts;
    private int[] ints;
    private long[] longs;
    private float[] floats;
    private double[] doubles;
    private boolean[] booleans;
    private char[] chars;

    public void addObject(Object valueToCollect) {
        if (values != null) {
            values.add(valueToCollect);
        } else if (value == null) {
            value = valueToCollect;
        } else {
            values = new ArrayList<Object>();
            values.add(value);
            values.add(valueToCollect);
            value = null;
        }
    }

    @Override
    public void addByte(byte value) {
        if(values != null) {

        } else {

        }
    }

    @Override
    public void addShort(float value) {
        if(values != null) {

        } else {

        }
    }

    @Override
    public void addInt(int value) {
        if(values != null) {

        } else {

        }
    }

    @Override
    public void addLong(long value) {
        if(values != null) {

        } else {

        }
    }

    @Override
    public void addFloat(float value) {
        if(values != null) {

        } else {

        }
    }

    @Override
    public void addDouble(double value) {
        if(values != null) {

        } else {

        }
    }

    @Override
    public void addBoolean(boolean value) {
        if(values != null) {

        } else {

        }
    }

    @Override
    public void addChar(char value) {
        if(values != null) {

        } else {

        }
    }

    public Object getResult() {
        if (value != null) {
            return value;
        } else if (values != null) {
            return new MultiResult<Object>(values);
        } else {
            return null;
        }
    }

}
