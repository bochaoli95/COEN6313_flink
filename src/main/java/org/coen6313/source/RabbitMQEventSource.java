package org.coen6313.source;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.coen6313.pojo.Event;

import java.time.Instant;

import java.util.Objects;

    public class RabbitMQEventSource {

        public static DataStream<Event> build(StreamExecutionEnvironment env) {

            RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                    .setHost("20.151.88.17")
                    .setPort(5672)
                    .setUserName("coen6313")
                    .setPassword("coen6313")
                    .setVirtualHost("/")
                    .build();

            ObjectMapper mapper = new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .setPropertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE);

            DataStream<String> jsonStream = env
                    .addSource(new RMQSource<>(
                            connectionConfig,
                            "user-events",
                            true,
                            new SimpleStringSchema()
                    ))
                    .setParallelism(1)
                    .name("RabbitMQ Source");

            return jsonStream
                    .map(json -> {
                        try {
                            Event event = mapper.readValue(json, Event.class);
                            event.setTimestamp(Instant.now().toEpochMilli());
                            System.out.println("[RabbitMQEventSource] ✅ Received: " + event);
                            return event;
                        } catch (Exception e) {
                            System.err.println("[RabbitMQEventSource] Parse failed: " + json + " | Error: " + e.getMessage());
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());
        }
    }


