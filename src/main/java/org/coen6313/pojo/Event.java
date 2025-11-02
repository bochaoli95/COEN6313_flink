package org.coen6313.pojo;

import java.io.Serializable;

public class Event implements Serializable {
    private String userId;
    private String eventType;
    private Long timestamp;
    private String eventDetail; // optional: detail like item id or action description

    public Event() {}

    public Event(String userId, String eventType, Long timestamp, String eventDetail) {
        this.userId = userId;
        this.eventType = eventType;
        this.timestamp = timestamp;
        this.eventDetail = eventDetail;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getEventDetail() {
        return eventDetail;
    }

    public void setEventDetail(String eventDetail) {
        this.eventDetail = eventDetail;
    }

    @Override
    public String toString() {
        return "Event{" +
                "userId='" + userId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                ", eventDetail='" + eventDetail + '\'' +
                '}';
    }
}
