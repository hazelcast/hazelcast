/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.pulsar.sql;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import java.io.Serializable;
import java.util.Objects;

public class AvroUser implements Serializable {
    public static final Schema SCHEMA = ReflectData.get().getSchema(AvroUser.class);

    private String name;
    private Integer favoriteNumber;

    public AvroUser() { }

    public AvroUser(String name, Integer favoriteNumber) {
        this.name = name;
        this.favoriteNumber = favoriteNumber;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getFavoriteNumber() {
        return favoriteNumber;
    }

    public void setFavoriteNumber(Integer favoriteNumber) {
        this.favoriteNumber = favoriteNumber;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AvroUser that = (AvroUser) obj;
        return Objects.equals(name, that.name) && Objects.equals(favoriteNumber, that.favoriteNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, favoriteNumber);
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", favoriteNumber=" + favoriteNumber +
                '}';
    }
}
