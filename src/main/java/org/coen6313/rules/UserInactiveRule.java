package org.coen6313.rules;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.coen6313.pojo.Event;
import org.coen6313.pojo.EventType;

public class UserInactiveRule {

    public static Pattern<Event, ?> getPattern() {
        return Pattern.<Event>begin("register")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        System.out.println(event.toString());
                        boolean match = "REGISTER".equalsIgnoreCase(event.getEventType());
                        if (match) System.out.println("🟢 [Pattern] REGISTER detected: " + event);
                        return match;
                    }
                })
                .notFollowedBy("chat")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return EventType.CHAT.getValue().equals(event.getEventType());
                    }
                })
                .within(Time.seconds(20));
    }
}
