package com.wixpress.dst.greyhound.java;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class CleanupPolicy {

    private CleanupPolicy() {}

    public abstract <A> A fold(Function<Duration, ? extends A> delete,
                               Supplier<? extends A> compact);

    public static CleanupPolicy delete(Duration retention) {
        return new CleanupPolicy() {
            @Override
            public <A> A fold(Function<Duration, ? extends A> delete,
                              Supplier<? extends A> compact) {
                return delete.apply(retention);
            }
        };
    }

    public static CleanupPolicy delete() {
        return new CleanupPolicy() {
            @Override
            public <A> A fold(Function<Duration, ? extends A> delete,
                              Supplier<? extends A> compact) {
                return compact.get();
            }
        };
    }

}
