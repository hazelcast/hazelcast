/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.json.internal;

/**
 * Represents the description of a Json terminal value. These are null,
 * true, false, number or string.
 */
public class JsonSchemaTerminalDescription extends JsonSchemaDescription {

    private int valueStartLocation;

    public JsonSchemaTerminalDescription(JsonSchemaStructDescription parent) {
        super(parent);
    }

    /**
     * Points to the location where this object starts in underlying
     * input stream. The returned value is offset from the start of
     * the object.
     *
     * @return
     */
    public int getValueStartLocation() {
        return valueStartLocation;
    }

    public void setValueStartLocation(int valueStartLocation) {
        this.valueStartLocation = valueStartLocation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        JsonSchemaTerminalDescription that = (JsonSchemaTerminalDescription) o;

        return valueStartLocation == that.valueStartLocation;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + valueStartLocation;
        return result;
    }

    @Override
    public String toString() {
        return "JsonSchemaLeafDescription{"
                + "valueStartLocation=" + valueStartLocation
                + '}';
    }
}
