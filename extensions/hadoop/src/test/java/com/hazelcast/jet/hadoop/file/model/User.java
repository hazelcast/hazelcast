/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.hadoop.file.model;

import org.apache.avro.reflect.AvroName;

import java.io.Serializable;
import java.util.Objects;

public class User implements Serializable {

    private String name;

    @AvroName("favorite_number") // TODO why do I need this?
    private int favoriteNumber;

    public User() {
    }

    public User(String name, int favoriteNumber) {
        this.name = name;
        this.favoriteNumber = favoriteNumber;
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
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", favoriteNumber=" + favoriteNumber +
                '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        User user = (User) o;
        return favoriteNumber == user.favoriteNumber &&
                Objects.equals(name, user.name);
    }

    @Override public int hashCode() {
        return Objects.hash(name, favoriteNumber);
    }

}
