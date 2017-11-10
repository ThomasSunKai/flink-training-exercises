package com.dataartisans.flinktraining.exercises.datastream_java.connectors;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.KafkaTestEnvironmentImpl;

public class KafkaSQL {
	public static final String TOPIC = "SQLtest";

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");

		KafkaTestEnvironmentImpl kafka = new KafkaTestEnvironmentImpl();
		kafka.prepare(1,false);
		kafka.createTestTopic(TOPIC, 1, 1);
		String brokerConnectionStrings = kafka.getBrokerConnectionString();
		System.out.println("ZK: " + kafka.getZookeeperConnectionString());
		System.out.println("Broker: " + brokerConnectionStrings);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		final int maxEventDelay = 60;       	// events are out of order by max 60 seconds
		final int servingSpeedFactor = 3600; 	// events of an hour are served every second
		DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

		DataStream<TaxiRide> filteredRides = rides
				// filter out rides that do not start or stop in NYC
				.filter(new RideCleansingToKafka.NYCFilter());

		// write the filtered data to a Kafka sink
		filteredRides.addSink(new FlinkKafkaProducer010<>(
				brokerConnectionStrings,
				TOPIC,
				new TaxiRideSchema()));

		// run the cleansing pipeline
		env.execute("Taxi Ride Cleansing");
	}
}
