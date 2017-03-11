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
 * A Segment contains data about an uninterrupted stretch of driving without stopping.
 *
 */
public class Segment {
	public Long startTime;
	public Long finishTime;
	public float maxSpeed;
	public long length;

	public Segment() {}

	public Segment(Iterable<ConnectedCarEvent> events) {
		TreeSet<ConnectedCarEvent> set = new TreeSet<ConnectedCarEvent>();

		this.finishTime = Segment.earliestStopEvent(events);

		for (Iterator<ConnectedCarEvent	> iterator = events.iterator(); iterator.hasNext(); ) {
			ConnectedCarEvent event = iterator.next();
			if (event.timestamp < this.finishTime) {
				set.add(event);
			}
		}

		this.length = set.size();
		if (this.length > 0) {
			this.startTime = set.first().timestamp;
			this.maxSpeed = Segment.maxSpeed(set);
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(startTime).append(",");
		sb.append(maxSpeed).append(",");
		sb.append(length);

		return sb.toString();
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof Segment &&
				this.startTime == ((Segment) other).startTime;
	}

	@Override
	public int hashCode() {
		return (int)this.startTime.hashCode();
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

	private static float maxSpeed(TreeSet<ConnectedCarEvent> set) {
		float max = 0;

		for (Iterator<ConnectedCarEvent> iterator = set.iterator(); iterator.hasNext(); ) {
			ConnectedCarEvent event = iterator.next();
			if (event.speed > max) max = event.speed;
		}
		return max;
	}
}
