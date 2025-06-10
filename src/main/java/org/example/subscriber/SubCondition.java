package org.example.subscriber;

import java.io.Serializable;
import java.text.MessageFormat;

public class SubCondition implements Serializable {
    public final String key;
    public final Operation operation;
    public final String value;

    public SubCondition(String key, Operation operation, String value) {
        this.key = key;
        this.operation = operation;
        this.value = value;
    }

    @Override
    public String toString() {
        return MessageFormat.format("({0} {1} {2})", key, operation.value, value.toString());
    }
}
