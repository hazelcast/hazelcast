/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class AddressUtil {
    
    private AddressUtil(){
    }
    
    public static List<String> handleMembers(final Collection<String> members){
        List<String> lsAddresses = new ArrayList<String>();
        for (final String address : members) {
            lsAddresses.addAll(AddressUtil.handleMember(address));
        }
        return lsAddresses;
    }

    public static List<String> handleMember(final String value) {
        final List<String> members = new ArrayList<String>();
        final Pattern ipPattern = Pattern.compile("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})\\.((\\*)||(\\d{1,3}-\\d{1,3}))");
        for (String token : value.split("[;, ]")) {
            token = token.trim();
            if (token.length() == 0) continue;
            
            final Matcher matcher = ipPattern.matcher(token);
            if (matcher.matches()){
                final String first3 = matcher.group(1);
                final String star = matcher.group(3);
                final String range = matcher.group(4);
                if (star != null && star.length() > 0) {
                    for (int j = 0; j < 256; j++) {
                        members.add(first3 + "." + j);
                    }
                } else if (range != null && range.length() > 0) {
                    final int dashPos = range.indexOf('-');
                    final int start = Integer.parseInt(range.substring(0, dashPos));
                    final int end = Integer.parseInt(range.substring(dashPos + 1));
                    for (int j = start; j <= end; j++) {
                        members.add(first3 + "." + j);
                    }
                } else {
                    members.add(token);
                }
            } else {
                members.add(token);
            }
        }
        return members;
    }
}
