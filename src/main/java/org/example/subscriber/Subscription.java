package org.example.subscriber;

import java.io.Serializable;
import java.util.List;

public class Subscription implements Serializable {
    public final String userId;
    public final List<SubCondition> conditions;

    public Subscription(String userId, List<SubCondition> conditions) {
        this.userId = userId;
        this.conditions = conditions;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append("{");

        for (SubCondition subCondition : conditions) {
            if(builder.length() != 1) {
                builder.append(", ");
            }

            builder.append(subCondition.toString());
        }

        builder.append("}");

        return builder.toString();
    }
}