/*
 * Copyright 2024 Hazelcast Inc.
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

package com.hazelcast.jet.avro.model;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import java.io.Serializable;
import java.util.Objects;

public class User implements Serializable {
    public static final Schema SCHEMA = ReflectData.get().getSchema(User.class);

    private String name;
    private int favoriteNumber;

    public User() { }

    public User(String name, int favoriteNumber) {
        this.name = name;
        this.favoriteNumber = favoriteNumber;
    }

    public static Schema classSchema() {
        return SCHEMA;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getFavoriteNumber() {
        return favoriteNumber;
    }

    public void setFavoriteNumber(int favoriteNumber) {
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
        User that = (User) obj;
        return Objects.equals(name, that.name) && favoriteNumber == that.favoriteNumber;
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
