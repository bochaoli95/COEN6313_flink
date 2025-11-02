package org.coen6313;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.coen6313.pojo.Event;
import org.coen6313.pojo.EventType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Engine {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> eventStream = env.addSource(new MockEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((SerializableTimestampAssigner<Event>)
                                        (event, ts) -> event.getTimestamp())
                );

        KeyedStream<Event, String> keyedStream = eventStream.keyBy(Event::getUserId);

        Pattern<Event, ?> pattern = Pattern.<Event>begin("register")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return EventType.REGISTER.getValue().equals(event.getEventType());
                    }
                })
                .notFollowedBy("click")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return EventType.CLICK.getValue().equals(event.getEventType());
                    }
                })
                .within(Time.seconds(5)); // 5 秒检测窗口

        PatternStream<Event> patternStream = CEP.pattern(keyedStream, pattern);

        SingleOutputStreamOperator<String> result = patternStream.select(
                (PatternSelectFunction<Event, String>) (Map<String, List<Event>> patternMap) -> {
                    Event registerEvent = patternMap.get("register").iterator().next();
                    return "⚠ User " + registerEvent.getUserId() + " registered but no click within 5s.";
                });

        result.print();

        env.execute("CEP - Detect Inactive Users (No Click After Register)");
    }

    // ===== 模拟事件源 =====
    public static class MockEventSource extends RichSourceFunction<Event> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            long now = System.currentTimeMillis();
            List<Event> events = new ArrayList<>();

            // user1 注册但不点击
            events.add(new Event("user1", EventType.REGISTER.getValue(), now, "user1 register"));
            // user2 注册后3秒点击
            events.add(new Event("user2", EventType.REGISTER.getValue(), now, "user2 register"));

            for (Event e : events) {
                ctx.collect(e);
            }

            // 3秒后 user2 点击（不会触发报警）
            Thread.sleep(3000);
            ctx.collect(new Event("user2", EventType.CLICK.getValue(), System.currentTimeMillis(), "user2 click"));

            // user1 一直不点，会触发警告
            Thread.sleep(7000); // 等待足够时间触发窗口超时
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
