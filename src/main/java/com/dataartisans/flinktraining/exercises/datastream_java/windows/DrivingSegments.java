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

package com.dataartisans.flinktraining.exercises.datastream_java.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.StoppedSegment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * Java reference implementation for the "Driving Segments" exercise of the Flink training
 * (http://dataartisans.github.io/flink-training).
 *
 * The task of the exercise is to divide the input stream of ConnectedCarEvents into segments,
 * where the car is being continuously driven without stopping.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class DrivingSegments {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// connect to the data file
		DataStream<String> carData = env.readTextFile(input);

        // map to events
		DataStream<ConnectedCarEvent> events = carData
				.map(new MapFunction<String, ConnectedCarEvent>() {
					@Override
					public ConnectedCarEvent map(String line) throws Exception {
						return ConnectedCarEvent.fromString(line);
					}
				})
				.assignTimestampsAndWatermarks(new ConnectedCarAssigner());

        // find segments
		events.keyBy("car_id")
		        .window(GlobalWindows.create())
				.trigger(new SegmentingOutOfOrderTrigger())
				.evictor(new SegmentingEvictor())
				.apply(new CreateStoppedSegment())
				.print();

		env.execute("Driving Segments");
	}

    // triggering would be much simpler if we didn't have to worry about out-of-order events
	public static class SegmentingInOrderTrigger extends Trigger<ConnectedCarEvent, GlobalWindow> {
		@Override
		public TriggerResult onElement(ConnectedCarEvent event, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
            if (event.speed == 0.0) {
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

		@Override
		public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}

		@Override
		public void clear(GlobalWindow window, TriggerContext ctx) { }
    }

	public static class SegmentingOutOfOrderTrigger extends Trigger<ConnectedCarEvent, GlobalWindow> {
		private final ValueStateDescriptor<TreeSet<Long>> stoppingTimesDesc =
				new ValueStateDescriptor<TreeSet<Long>>("stopping-times-desc",
						TypeInformation.of(new TypeHint<TreeSet<Long>>() {}));

		@Override
		public TriggerResult onElement(ConnectedCarEvent event,
									   long timestamp,
									   GlobalWindow window,
									   TriggerContext ctx) throws Exception {

            // keep track of events where the car is stopped
			ValueState<TreeSet<Long>> stoppingTimes = ctx.getPartitionedState(stoppingTimesDesc);
			TreeSet<Long> setOfTimes = stoppingTimes.value();

			// add a new stopped event to our Set
			if (event.speed == 0.0) {
				if (setOfTimes == null) {
					setOfTimes = new TreeSet<Long>();
				}
				setOfTimes.add(event.timestamp);
				stoppingTimes.update(setOfTimes);
			}

			// trigger the window when the watermark passes the earliest stop event
			if (setOfTimes != null && !setOfTimes.isEmpty()) {
				java.util.Iterator<Long> iter = setOfTimes.iterator();
				long nextStop = iter.next();
				if (ctx.getCurrentWatermark() >= nextStop) {
					iter.remove();
					stoppingTimes.update(setOfTimes);
					return TriggerResult.FIRE;
				}
			}

            // otherwise continue
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}

		@Override
		public void clear(GlobalWindow window, TriggerContext ctx) {
			ctx.getPartitionedState(stoppingTimesDesc).clear();
		}
	}

	public static class SegmentingEvictor implements Evictor<ConnectedCarEvent, GlobalWindow> {

		@Override
		public void evictBefore(Iterable<TimestampedValue<ConnectedCarEvent>> events,
								int size, GlobalWindow window, EvictorContext ctx) {
		}

		@Override
		public void evictAfter(Iterable<TimestampedValue<ConnectedCarEvent>> elements,
							   int size, GlobalWindow window, EvictorContext ctx) {
			long firstStop = ConnectedCarEvent.earliestStopElement(elements);

            // remove all events up to the first stop event (which is the event that triggered the window)
			for (Iterator<TimestampedValue<ConnectedCarEvent>> iterator = elements.iterator(); iterator.hasNext();) {
				TimestampedValue<ConnectedCarEvent> element = iterator.next();
				if (element.getTimestamp() <= firstStop) {
					iterator.remove();
				}
			}
		}
	}

	public static class ConnectedCarAssigner implements AssignerWithPunctuatedWatermarks<ConnectedCarEvent> {
		@Override
		public long extractTimestamp(ConnectedCarEvent event, long previousElementTimestamp) {
			return event.timestamp;
		}

		@Override
		public Watermark checkAndGetNextWatermark(ConnectedCarEvent event, long extractedTimestamp) {
            // simply emit a watermark with every event
			return new Watermark(extractedTimestamp - 30000);
		}
	}

	public static class CreateStoppedSegment implements WindowFunction<ConnectedCarEvent, StoppedSegment, Tuple, GlobalWindow> {
		@Override
		public void apply(Tuple key, GlobalWindow window, Iterable<ConnectedCarEvent> events, Collector<StoppedSegment> out) {
			StoppedSegment seg = new StoppedSegment(events);
			if (seg.length > 0) {
				out.collect(seg);
			}
		}

	}
}
