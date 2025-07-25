package org.example.subscriber;

import java.util.Random;

import org.example.util.RandomUtil;

public enum Operation {
    LOWER("<"),
    LOWER_EQUAL("<="),
    EQUAL("="),
    GREATER_EQUAL(">"),
    GREATER(">=");

    public final String value;

    Operation(String string) {
        this.value = string;
    }

    public static Operation[] valuesNoEqual() {
        return new Operation[]{ LOWER, LOWER_EQUAL, GREATER, GREATER_EQUAL };
    }

    public static Operation randomEqual(double chance) {
        var isEqual = new Random().nextDouble() < chance;

        return isEqual ? Operation.EQUAL : RandomUtil.randomFrom(valuesNoEqual());
    }

    public static boolean compare(String aObj, Operation operation, String bObj) {
        var a = Double.parseDouble(aObj);
        var b = Double.parseDouble(bObj);

        switch (operation) {
            case LOWER:
                return a < b;

            case LOWER_EQUAL:
                return a <= b;

            case EQUAL:
                return a == b;

            case GREATER_EQUAL:
                return a >= b;

            case GREATER:
                return a > b;
        
            default:
                return false;
        }
    }
}
