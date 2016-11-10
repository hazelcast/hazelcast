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

package com.hazelcast.jet.cascading;

import cascading.flow.Flow;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.cascading.tap.InternalMapTap;
import com.hazelcast.jet2.JetEngineConfig;
import org.junit.Ignore;
import org.junit.Test;

public class WordCountExample {


    @Test
    @Ignore
    public void wordCount() {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        IMap<Integer, String> sourceMap = instance1.getMap("sourceMap");
        IMap<String, Integer> sinkMap = instance1.getMap("sinkMap");

        sourceMap.put(0, "It was the best of times, "
                + "it was the worst of times, "
                + "it was the age of wisdom, "
                + "it was the age of foolishness, "
                + "it was the epoch of belief, "
                + "it was the epoch of incredulity, "
                + "it was the season of Light, "
                + "it was the season of Darkness, "
                + "it was the spring of hope, "
                + "it was the winter of despair, "
                + "we had everything before us, "
                + "we had nothing before us, "
                + "we were all going direct to Heaven, "
                + "we were all going direct the other way-- "
                + "in short, the period was so far like the present period, that some of "
                + "its noisiest authorities insisted on its being received, for good or for "
                + "evil, in the superlative degree of comparison only.");

        sourceMap.put(1, "There were a king with a large jaw and a queen with a plain face, on the "
                + "throne of England; there were a king with a large jaw and a queen with "
                + "a fair face, on the throne of France. In both countries it was clearer "
                + "than crystal to the lords of the State preserves of loaves and fishes, "
                + "that things in general were settled for ever.");

        sourceMap.put(2, "It was the year of Our Lord one thousand seven hundred and seventy-five. "
                + "Spiritual revelations were conceded to England at that favoured period, "
                + "as at this. Mrs. Southcott had recently attained her five-and-twentieth "
                + "blessed birthday, of whom a prophetic private in the Life Guards had "
                + "heralded the sublime appearance by announcing that arrangements were "
                + "made for the swallowing up of London and Westminster. Even the Cock-lane "
                + "ghost had been laid only a round dozen of years, after rapping out its "
                + "messages, as the spirits of this very year last past (supernaturally "
                + "deficient in originality) rapped out theirs. Mere messages in the "
                + "earthly order of events had lately come to the English Crown and People, "
                + "from a congress of British subjects in America: which, strange "
                + "to relate, have proved more important to the human race than any "
                + "communications yet received through any of the chickens of the Cock-lane "
                + "brood.");

        InternalMapTap sourceTap = new InternalMapTap(sourceMap.getName(),
                new KeyValuePair(new Fields("number", "line")));
        InternalMapTap sinkTap = new InternalMapTap(sinkMap.getName(),
                new KeyValuePair(new Fields("token", "count")), SinkMode.REPLACE);

        Fields line = new Fields("line");
        Fields token = new Fields("token");
        RegexSplitGenerator splitter = new RegexSplitGenerator(token, "[ \\[\\]\\(\\),.]");
        // only returns "token"
        Pipe wcPipe = new Each("token", line, splitter, Fields.RESULTS);
        wcPipe = new GroupBy(wcPipe, token);
        wcPipe = new Every(wcPipe, Fields.ALL, new Count(), Fields.ALL);


        JetFlowConnector flowConnector = new JetFlowConnector(instance1, new JetEngineConfig());
        Flow flow = flowConnector.connect(sourceTap, sinkTap, wcPipe);
        flow.complete();

        System.out.println(sinkMap.entrySet());
        Hazelcast.shutdownAll();
    }

}
