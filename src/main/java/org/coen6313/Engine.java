package org.coen6313;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.coen6313.pojo.Event;
import org.coen6313.pojo.EventType;
import org.coen6313.rules.UserInactiveRule;
import org.coen6313.sink.RabbitMQEventSink;
import org.coen6313.source.RabbitMQEventSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Engine {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStream<Event> eventStream = env.addSource(new MockEventSource())
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
//                                .withTimestampAssigner((SerializableTimestampAssigner<Event>)
//                                        (event, ts) -> event.getTimestamp())
//                );
        DataStream<Event> eventStream = RabbitMQEventSource.build(env);
        eventStream.print("Input Event");

        KeyedStream<Event, String> keyedStream = eventStream.keyBy(Event::getUserId);

        PatternStream<Event> patternStream =
                CEP.pattern(keyedStream, UserInactiveRule.getPattern())
                        .inProcessingTime();
        SingleOutputStreamOperator<String> result = patternStream.select(
                (PatternSelectFunction<Event, String>) (Map<String, List<Event>> patternMap) -> {
                    System.out.println("[Pattern Matched] " + patternMap);
                    Event registerEvent = patternMap.get("register").iterator().next();
                    String userId = registerEvent.getUserId();
                    return "User " + userId + " registered but no chat within 5s.";
                });

        result.print("CEP Output");
        result.addSink(RabbitMQEventSink.build()).name("RabbitMQ Sink");

        env.execute("CEP - Detect Inactive Users");
    }

    public static class MockEventSource extends RichSourceFunction<Event> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            long now = System.currentTimeMillis();
            List<Event> events = new ArrayList<>();

            events.add(new Event("user1", EventType.REGISTER.getValue(), now, "user1 register"));
            events.add(new Event("user2", EventType.REGISTER.getValue(), now, "user2 register"));

            for (Event e : events) {
                System.out.println("🟢 Emitting: " + e);
                ctx.collect(e);
            }

            Thread.sleep(3000);
            ctx.collect(new Event("user2", EventType.CHAT.getValue(), System.currentTimeMillis(), "user2 chat"));

            Thread.sleep(7000);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
