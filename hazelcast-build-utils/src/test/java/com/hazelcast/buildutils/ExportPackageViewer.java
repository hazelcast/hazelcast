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

package com.hazelcast.buildutils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExportPackageViewer {

    public static void main(String[] args)
            throws Exception {

        String sourcefile = args[0];
        File file = new File(sourcefile);
        FileReader fileReader = new FileReader(file);

        BufferedReader reader = new LineNumberReader(fileReader);
        String data = reader.readLine();

        List<String> strings = ElementParser.parseDelimitedString(data, ',');
        Set<String> packages = new HashSet<String>();
        for (String entry : strings) {
            int usesIndex = entry.indexOf(";");
            if (usesIndex != -1) {
                entry = entry.substring(0, usesIndex);
            }

            packages.add(entry);
        }

        List<String> result = new ArrayList<String>(packages);
        Collections.sort(result);

        for (String p : result) {
            System.out.println(p);
        }
    }

}
