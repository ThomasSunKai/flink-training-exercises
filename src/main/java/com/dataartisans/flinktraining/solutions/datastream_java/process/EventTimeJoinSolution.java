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

package com.dataartisans.flinktraining.solutions.datastream_java.process;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Customer;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.EnrichedTrade;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Trade;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.FinSources;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class EventTimeJoinSolution {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Simulated trade stream
		DataStream<Trade> tradeStream = FinSources.tradeSource(env);

		// Simulated customer stream
		DataStream<Customer> customerStream = FinSources.customerSource(env);

		// Stream of enriched trades
		DataStream<EnrichedTrade> joinedStream = tradeStream
				.keyBy("customerId")
				.connect(customerStream.keyBy("customerId"))
				.process(new EventTimeJoinFunction());

		joinedStream.print();

		env.execute("Event-time join");
	}

	/**
	 * This is an event-time-based enrichment join implemented with a CoProcessFunction.
	 *
	 * When we receive a trade we want to join it against the customer data that was knowable
	 * at the time of the trade. This means that we ignore customer data with a timestamp newer than
	 * the timestamp of the trade, and we wait until the current watermark (the event-time clock) reaches
	 * the timestamp of the trade.
	 *
	 * This will give us deterministic results even in the face of out-of-order data (so long as nothing is late).
	 */

	public static class EventTimeJoinFunction extends CoProcessFunction<Trade, Customer, EnrichedTrade> {
		private MapState<Long, Trade> tradeMap = null;
		private MapState<Long, Customer> customerMap = null;

		@Override
		public void open(Configuration config) {
			MapStateDescriptor tDescriptor = new MapStateDescriptor<Long, Trade>(
					"tradeBuffer",
					TypeInformation.of(Long.class),
					TypeInformation.of(Trade.class)
			);
			tradeMap = getRuntimeContext().getMapState(tDescriptor);

			MapStateDescriptor cDescriptor = new MapStateDescriptor<Long, Customer>(
					"customerBuffer",
					TypeInformation.of(Long.class),
					TypeInformation.of(Customer.class)
			);
			customerMap = getRuntimeContext().getMapState(cDescriptor);
		}

		@Override
		public void processElement1(Trade trade,
									Context context,
									Collector<EnrichedTrade> out)
				throws Exception {

			System.out.println("Received " + trade.toString());
			TimerService timerService = context.timerService();

			if (context.timestamp() > timerService.currentWatermark()) {
				tradeMap.put(trade.timestamp, trade);
				timerService.registerEventTimeTimer(trade.timestamp);
			} else {
				// handle late Trades
			}
		}

		@Override
		public void processElement2(Customer customer,
									Context context,
									Collector<EnrichedTrade> collector)
				throws Exception {

			System.out.println("Received " + customer.toString());
			customerMap.put(customer.timestamp, customer);
		}

		@Override
		public void onTimer(long t,
							OnTimerContext context,
							Collector<EnrichedTrade> out)
				throws Exception {

			Trade trade = tradeMap.get(t);
			if (trade != null) {
				tradeMap.remove(t);
				EnrichedTrade joined = new EnrichedTrade(trade, getCustomerRecordToJoin(trade));
				out.collect(joined);
			}
		}

		private Customer getCustomerRecordToJoin(Trade trade) throws Exception {
			Iterator<Map.Entry<Long, Customer>> customerEntries = customerMap.entries().iterator();
			List<Long> keysToRemove = new ArrayList<Long>();
			Customer newestNotAfterTrade = null;
			boolean afterTrade = false;

			while(customerEntries.hasNext() && !afterTrade) {
				Customer c = customerEntries.next().getValue();
				if (c.timestamp <= trade.timestamp) {
					if (newestNotAfterTrade != null) {
						keysToRemove.add(newestNotAfterTrade.timestamp);
					}
					newestNotAfterTrade = c;
				} else {
					afterTrade = true;
				}
			}

			for (Long t : keysToRemove) {
				System.out.println("Removing customer @ " + t);
				customerMap.remove(t);
			}

			return newestNotAfterTrade;
		}
	}
}
