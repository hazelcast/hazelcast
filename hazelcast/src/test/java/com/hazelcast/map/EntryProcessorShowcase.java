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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class EntryProcessorShowcase {


    public static void main(String[] args) {
        Config config = new Config();
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        IMap<Integer, Game> games = instance.getMap("games");
        fillGames(games);

        addScore(games, 1, new User("husain123"), 1001);

        System.out.println(games.get(1));
    }


    private static void addScore(IMap<Integer, Game> games, int gameId, User user, int score) {
        games.executeOnKey(gameId, new AddScoreEntryProcessor(user, score));

    }

    static class AddScoreEntryProcessor implements EntryProcessor, EntryBackupProcessor, Serializable {

        User user;
        int score;

        AddScoreEntryProcessor(User user, int score) {
            this.user = user;
            this.score = score;
        }

        public Object process(Map.Entry entry) {
            Game game = (Game) entry.getValue();
            game.scoreMap.get(user).add(new Score(score, new Date()));
            return true;
        }

        public EntryBackupProcessor getBackupProcessor() {
            return AddScoreEntryProcessor.this;
        }

        public void processBackup(Map.Entry entry) {
            process(entry);
        }
    }


    private static void fillGames(IMap<Integer, Game> games) {
        Game game = new Game();
        game.name = "The Game";
        HashMap<User, List<Score>> userMap = new HashMap<User, List<Score>>();
        ArrayList<Score> scores = new ArrayList<Score>();
        User user = new User("husain123", "Husain Bolt", "hbolt@gahoo.com", new Date(), new Date());
        userMap.put(user, scores);
        game.scoreMap = userMap;
        games.put(1, game);
    }


    public static class Game implements DataSerializable {
        String name;
        Map<User, List<Score>> scoreMap;

        public Game() {
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
            int size = scoreMap.size();
            out.writeInt(size);
            for (User user : scoreMap.keySet()) {
                out.writeObject(user);
                List<Score> scores = scoreMap.get(user);
                int lsize = scores.size();
                out.writeInt(lsize);
                for (Score score : scores) {
                    out.writeObject(score);
                }
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readUTF();
            int size = in.readInt();
            scoreMap = new HashMap<User, List<Score>>();
            for (int i = 0; i < size; i++) {
                User user = in.readObject();
                int lsize = in.readInt();
                List<Score> scores = new ArrayList<Score>(lsize);
                for (int j = 0; j < lsize; j++) {
                    scores.add(in.<Score>readObject());
                }
                scoreMap.put(user, scores);
            }
        }

        @Override
        public String toString() {
            return "Game{" +
                    "name='" + name + '\'' +
                    ", scoreMap=" + scoreMap +
                    '}';
        }
    }

    public static class User implements DataSerializable {
        String userId;
        String name;
        String email;
        Date firstActionDate;
        Date latestActionDate;

        public User() {
        }

        public User(String userId) {
            this.userId = userId;
        }

        public User(String userId, String name, String email, Date firstActionDate, Date latestActionDate) {
            this.userId = userId;
            this.name = name;
            this.email = email;
            this.firstActionDate = firstActionDate;
            this.latestActionDate = latestActionDate;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(userId);
            out.writeUTF(name);
            out.writeUTF(email);
            out.writeLong(firstActionDate.getTime());
            out.writeLong(latestActionDate.getTime());
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            userId = in.readUTF();
            name = in.readUTF();
            email = in.readUTF();
            firstActionDate = new Date(in.readLong());
            latestActionDate = new Date(in.readLong());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            User user = (User) o;

            if (userId != null ? !userId.equals(user.userId) : user.userId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return userId != null ? userId.hashCode() : 0;
        }
    }

    public static class Score implements DataSerializable {

        long score;
        Date scoreDate;

        public Score() {
        }

        public Score(long score, Date scoreDate) {
            this.score = score;
            this.scoreDate = scoreDate;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(score);
            out.writeLong(scoreDate.getTime());
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            score = in.readLong();
            scoreDate = new Date(in.readLong());
        }
    }

}
