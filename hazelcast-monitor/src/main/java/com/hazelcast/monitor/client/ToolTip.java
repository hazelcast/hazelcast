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

package com.hazelcast.monitor.client;

import com.google.gwt.event.logical.shared.CloseEvent;
import com.google.gwt.event.logical.shared.CloseHandler;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.PopupPanel;

public class ToolTip extends PopupPanel {

    final int VISIBLE_DELAY = 2000;

    Timer removeDelay;

    public ToolTip(String message, int x, int y) {
        super(true);
        this.setPopupPosition(x, y);
        this.add(new Label(message));
        removeDelay = new Timer() {
            public void run() {
                ToolTip.this.setVisible(false);
                ToolTip.this.hide();
            }
        };
        removeDelay.schedule(VISIBLE_DELAY);
        this.addCloseHandler(new CloseHandler() {

            public void onClose(CloseEvent closeEvent) {
                removeDelay.cancel();
            }
        });
        this.setStyleName("toolTip");
        this.show();
    }

//    public boolean onEventPreview(Event event) {
//        int type = DOM.eventGetType(event);
//        switch (type) {
//            case Event.ONMOUSEDOWN:
//            case Event.ONCLICK: {
//                this.hide();
//                return true;
//            }
//        }
//        return false;
//    }
}