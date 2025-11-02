package org.coen6313.pojo;

public enum EventType {
    REGISTER("REGISTER"),
    LOGIN("LOGIN"),
    CLICK("CLICK"),
    VIEW("VIEW"),
    PURCHASE("PURCHASE"),
    LOGOUT("LOGOUT");

    private final String value;

    EventType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static EventType fromString(String value) {
        for (EventType type : EventType.values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown event type: " + value);
    }

    @Override
    public String toString() {
        return value;
    }
}
