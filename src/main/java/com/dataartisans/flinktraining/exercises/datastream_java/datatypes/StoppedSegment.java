/*
 * Copyright 2017 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.exercises.datastream_java.datatypes;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * A StoppedSegment is punctuated by the car stopping.
 *
 */
public class StoppedSegment extends Segment {

    public StoppedSegment(Iterable<ConnectedCarEvent> events) {
        long finishTime = StoppedSegment.earliestStopEvent(events);

        TreeSet<ConnectedCarEvent> set = new TreeSet<ConnectedCarEvent>();
        for (Iterator<ConnectedCarEvent	> iterator = events.iterator(); iterator.hasNext(); ) {
            ConnectedCarEvent event = iterator.next();
            if (event.timestamp < finishTime) {
                set.add(event);
            }
        }

        ConnectedCarEvent[] array = set.toArray(new ConnectedCarEvent[set.size()]);
        this.length = array.length;

        if (this.length > 0) {
            this.startTime = array[0].timestamp;
            this.maxSpeed = (int) Segment.maxSpeed(array);
            this.erraticness = Segment.stddevThrottle(array);
        }
    }

    private static Long earliestStopEvent(Iterable<ConnectedCarEvent> events) {
        long earliestTime = Long.MAX_VALUE;

        for (Iterator<ConnectedCarEvent	> iterator = events.iterator(); iterator.hasNext(); ) {
            ConnectedCarEvent event = iterator.next();
            if (event.timestamp < earliestTime && event.speed == 0.0) {
                earliestTime = event.timestamp;
            }
        }

        return earliestTime;
    }

}
