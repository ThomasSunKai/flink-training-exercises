/*
 * Copyright 2018 data Artisans GmbH
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
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

public class EventTimeJoinSolution {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath("file:///tmp/foo");
		env.setStateBackend(rocksDBStateBackend);

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

	/*
	 * This is an event-time-based enrichment join implemented with a CoProcessFunction, used to enrich a
	 * stream of financial Trades with Customer data.
	 *
	 * When we receive a Trade we want to join it with the Customer data that was knowable
	 * at the time of the Trade. This means that we ignore Customer data with timestamps after the
	 * the timestamp of the trade. And we wait until the current watermark (the event-time clock)
	 * reaches the timestamp of the trade before doing the join, so that races between the
	 * two streams can resolve themselves.
	 *
	 * Assuming the watermarking is correct, and there is no late data, this will behave deterministically.
	 */

	public static class EventTimeJoinFunction extends CoProcessFunction<Trade, Customer, EnrichedTrade> {
		// Store pending Trades for a customerId, keyed by timestamp
		private MapState<Long, Trade> tradeMap = null;

		// Store Customer updates for a customerId, keyed by timestamp
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
				// Do the join later, by which time any relevant Customer records should have have arrived.
				tradeMap.put(trade.timestamp, trade);
				timerService.registerEventTimeTimer(trade.timestamp);
			} else {
				// Late Trades land here.
			}
		}

		@Override
		public void processElement2(Customer customer,
									Context context,
									Collector<EnrichedTrade> collector)
				throws Exception {

			System.out.println("Received " + customer.toString());
			customerMap.put(customer.timestamp, customer);

			/* Calling this solely for its side effect of freeing older Customer records.
			 * Otherwise Customers with frequent updates and no Trades would leak state.
			 */
			getCustomerRecordToJoin(context.timerService().currentWatermark());
		}

		@Override
		public void onTimer(long t,
							OnTimerContext context,
							Collector<EnrichedTrade> out)
				throws Exception {

			Trade trade = tradeMap.get(t);
			if (trade != null) {
				tradeMap.remove(t);
				EnrichedTrade joined = new EnrichedTrade(trade, getCustomerRecordToJoin(trade.timestamp));
				out.collect(joined);
			}
		}

		/*
		 * Returns the newest Customer that isn't newer than the Trade we are enriching.
		 * As a side effect, removes earlier Customer records.
		 *
		 * This depends on the RocksDBStateBackend's implementation of the iterator()
		 * method on MapState, which iterates entries in the order of the (binary) keys.
		 */
		private Customer getCustomerRecordToJoin(Long timestamp) throws Exception {
			Iterator<Map.Entry<Long, Customer>> customerEntries = customerMap.iterator();
			Customer newestNotAfterTS = null;

			while(customerEntries.hasNext()) {
				Customer c = customerEntries.next().getValue();
				if (c.timestamp <= timestamp) {
					/*
					* By the time Trades are being processed in onTimer(), the Trades are being
					* processed in order (by Timestamp).
					* This means that any Customer record too old to join with this Trade is too old
					* to be worth keeping any longer.
					*/
					if (newestNotAfterTS != null) {
						System.out.println("Removing customer @ " + newestNotAfterTS.timestamp);
						customerMap.remove(newestNotAfterTS.timestamp);
					}
					newestNotAfterTS = c;
				} else {
					break;
				}
			}

			return newestNotAfterTS;
		}
	}
}
