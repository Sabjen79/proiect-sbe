package org.example.subscriber;

import java.io.Serializable;
import java.text.MessageFormat;

public class SubCondition implements Serializable {
    public String key;
    public Operation operation;
    public String value;
    public int type; // 0 - String, 1 - Long, 2 - Double

    public SubCondition(String key, Operation operation, String value, int type) {
        this.key = key;
        this.operation = operation;
        this.value = value;
        this.type = type;
    }

    @Override
    public String toString() {
        return MessageFormat.format("({0} {1} {2})", key, operation.value, value.toString());
    }
}
