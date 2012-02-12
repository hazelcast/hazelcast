/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.web.tomcat;

import org.apache.catalina.session.StandardSessionFacade;

import javax.servlet.http.HttpSession;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * @author ali
 */

public class HazelcastSessionFacade extends StandardSessionFacade {

    /**
     * Wrapped session object.
     */
    private HttpSession session = null;

    public HazelcastSessionFacade(HazelcastSession session) {
        super(session);
        this.session = session;
    }

    public List<HazelcastAttribute> getTouchedAttributes(long requestId) {
        List<HazelcastAttribute> touchedList = new ArrayList<HazelcastAttribute>();
        Enumeration<String> attNames = session.getAttributeNames();
        HazelcastSession hazelcastSession = (HazelcastSession) session;
        while (attNames.hasMoreElements()) {
            String attName = attNames.nextElement();
            HazelcastAttribute hattribute = (HazelcastAttribute) hazelcastSession.getLocalAttribute(attName);
            if (hattribute != null && hattribute.isTouched(requestId)) {
                touchedList.add(hattribute);
            }
        }
        return touchedList;
    }
}
