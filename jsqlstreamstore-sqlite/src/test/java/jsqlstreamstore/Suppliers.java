package jsqlstreamstore;

import com.google.common.annotations.VisibleForTesting;
import jsqlstreamstore.infrastructure.Ensure;

import java.util.function.Supplier;

final class Suppliers {

    private Suppliers() {
        // statics only
    }

    public static <T> Supplier<T> memoize(Supplier<T> delegate) {
        if (delegate instanceof MemoizingSupplier) {
            return delegate;
        }
        return new MemoizingSupplier<>(delegate);
    }

    @VisibleForTesting
    static class MemoizingSupplier<T> implements Supplier<T> {
        final Supplier<T> delegate;
        volatile boolean initialized;
        T value;

        MemoizingSupplier(Supplier<T> delegate) {
            this.delegate = Ensure.notNull(delegate);
        }

        @Override
        public T get() {
            if (!initialized) {
                synchronized (this) {
                    if (!initialized) {
                        T t = delegate.get();
                        value = t;
                        initialized = true;
                        return t;
                    }
                }
            }
            return value;
        }

        @Override
        public String toString() {
            return "Suppliers.memoize("
                + (initialized ? "<supplier that returned " + value + ">" : delegate)
                + ")";
        }
    }

}
