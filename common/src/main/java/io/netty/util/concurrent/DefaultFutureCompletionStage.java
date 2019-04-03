/*
 * Copyright 2019 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util.concurrent;


import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a {@link Future} and provides a {@link CompletionStage} implementation on top of it.
 *
 * @param <V> the value type.
 */
final class DefaultFutureCompletionStage<V> implements FutureCompletionStage<V> {
    private enum Marker {
        EMPTY,
        ERROR
    }

    private final Future<V> future;

    DefaultFutureCompletionStage(Future<V> future) {
        this.future = future;
    }

    @Override
    public Future<V> future() {
        return future;
    }

    @Override
    public <U> CompletionStage<U> thenApplyAsync(Function<? super V, ? extends U> fn, Executor executor) {
        requireNonNull(fn, "fn");
        requireNonNull(executor, "executor");

        Promise<U> promise = executor().newPromise();
        future.addListener(future -> {
            Throwable cause = future.cause();
            if (cause == null) {
                @SuppressWarnings("unchecked") V value = (V) future.getNow();
                if (executeDirectly(executor)) {
                    thenApplyAsync0(promise, value, fn);
                } else {
                    safeExecute(executor, () -> thenApplyAsync0(promise, value, fn), promise);
                }
            } else {
                promise.setFailure(cause);
            }
        });
        return promise.asStage();
    }

    private static <U, V> void thenApplyAsync0(Promise<U> promise, V value, Function<? super V, ? extends U> fn) {
        final U result;
        try {
            result = fn.apply(value);
        } catch (Throwable cause) {
            promise.setFailure(cause);
            return;
        }
        promise.setSuccess(result);
    }

    @Override
    public CompletionStage<Void> thenAcceptAsync(Consumer<? super V> action, Executor executor) {
        requireNonNull(action, "action");
        requireNonNull(executor, "executor");

        Promise<Void> promise = executor().newPromise();
        future.addListener(future -> {
            Throwable cause = future.cause();
            if (cause == null) {
                @SuppressWarnings("unchecked") V value = (V) future.getNow();
                if (executeDirectly(executor)) {
                    thenAcceptAsync0(promise, value, action);
                } else {
                    safeExecute(executor, () -> thenAcceptAsync0(promise, value, action), promise);
                }
            } else {
                promise.setFailure(cause);
            }
        });
        return promise.asStage();
    }

    private static <U, V> void thenAcceptAsync0(Promise<U> promise, V value, Consumer<? super V> action) {
        try {
            action.accept(value);
            promise.setSuccess(null);
        } catch (Throwable cause) {
            promise.setFailure(cause);
        }
    }

    @Override
    public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor) {
        return thenAcceptAsync(ignore -> action.run(), executor);
    }

    @Override
    public <U, V1> CompletionStage<V1> thenCombineAsync(
            CompletionStage<? extends U> other, BiFunction<? super V, ? super U, ? extends V1> fn, Executor executor) {
        requireNonNull(other, "other");
        requireNonNull(fn, "fn");
        requireNonNull(executor, "executor");

        Promise<V1> promise = executor().newPromise();
        AtomicReference<Object> reference = new AtomicReference<>(Marker.EMPTY);

        abstract class CombineBiConsumer<T1, T2, T> implements BiConsumer<T, Throwable> {
            @SuppressWarnings("unchecked")
            @Override
            public void accept(T v, Throwable error) {
                if (error == null) {
                    if (!reference.compareAndSet(Marker.EMPTY, v)) {
                        Object rawValue = reference.get();
                        if (rawValue == Marker.ERROR) {
                            assert promise.isDone();
                            return;
                        }
                        applyAndNotify0(promise, (T1) v, (T2) rawValue, fn);
                    }
                } else if (reference.compareAndSet(Marker.EMPTY, Marker.ERROR)) {
                    promise.setFailure(error);
                }
            }

            abstract void applyAndNotify0(
                    Promise<V1> promise, T1 value1, T2 value2, BiFunction<? super V, ? super U, ? extends V1> fn);
        }

        whenCompleteAsync(new CombineBiConsumer<V, U, V>() {
            @Override
            void applyAndNotify0(
                    Promise<V1> promise, V value1, U value2, BiFunction<? super V, ? super U, ? extends V1> fn) {
                applyAndNotify(promise, value1, value2, fn);
            }
        }, executor);
        other.whenCompleteAsync(new CombineBiConsumer<U, V, U>() {
            @Override
            void applyAndNotify0(
                    Promise<V1> promise, U value1, V value2, BiFunction<? super V, ? super U, ? extends V1> fn) {
                applyAndNotify(promise, value2, value1, fn);
            }
        }, executor);
        return promise.asStage();
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBothAsync(
            CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action, Executor executor) {
        requireNonNull(action, "action");
        return thenCombineAsync(other, (value, error) -> {
            action.accept(value, error);
            return null;
        }, executor);
    }

    @Override
    public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        requireNonNull(action, "action");
        return thenCombineAsync(other, (ignoreValue, ignoreError) -> {
            action.run();
            return null;
        }, executor);
    }

    @Override
    public <U> CompletionStage<U> applyToEitherAsync(
            CompletionStage<? extends V> other, Function<? super V, U> fn, Executor executor) {
        requireNonNull(other, "other");
        requireNonNull(fn, "fn");

        Promise<U> promise = executor().newPromise();
        BiConsumer<V, Throwable> consumer = new AtomicBiConsumer<V, U>(promise) {
            @Override
            protected U apply(V value) {
                return fn.apply(value);
            }
        };
        whenCompleteAsync(consumer, executor);
        other.whenCompleteAsync(consumer, executor);
        return promise.asStage();
    }

    @Override
    public CompletionStage<Void> acceptEitherAsync(
            CompletionStage<? extends V> other, Consumer<? super V> action, Executor executor) {
        requireNonNull(other, "other");
        requireNonNull(action, "action");

        Promise<Void> promise = executor().newPromise();
        BiConsumer<V, Throwable> consumer = new AtomicBiConsumer<V, Void>(promise) {
            @Override
            protected Void apply(V value) {
                action.accept(value);
                return null;
            }
        };
        whenCompleteAsync(consumer, executor);
        other.whenCompleteAsync(consumer, executor);
        return promise.asStage();
    }

    @Override
    public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        requireNonNull(other, "other");
        requireNonNull(action, "action");

        Promise<Void> promise = executor().newPromise();
        BiConsumer<Object, Throwable> consumer = new AtomicBiConsumer<Object, Void>(promise) {
            @Override
            protected Void apply(Object value) {
                action.run();
                return null;
            }
        };
        whenCompleteAsync(consumer, executor);
        other.whenCompleteAsync(consumer, executor);
        return promise.asStage();
    }

    @Override
    public <U> CompletionStage<U> thenComposeAsync(
            Function<? super V, ? extends CompletionStage<U>> fn, Executor executor) {
        requireNonNull(fn, "fn");
        requireNonNull(executor, "executor");

        Promise<U> promise = executor().newPromise();
        future.addListener(f -> {
           Throwable cause = f.cause();
           if (cause == null) {
               @SuppressWarnings("unchecked") V value = (V) f.getNow();
               if (executeDirectly(executor)) {
                   thenComposeAsync0(promise, fn, value);
               } else {
                   safeExecute(executor, () -> thenComposeAsync0(promise, fn, value), promise);
               }
           } else {
               promise.setFailure(cause);
           }
        });
        return promise.asStage();
    }

    private static <V, U> void thenComposeAsync0(
            Promise<U> promise, Function<? super V, ? extends CompletionStage<U>> fn, V value) {
        fn.apply(value).whenComplete((v, error) -> {
            if (error == null) {
                promise.setSuccess(v);
            } else {
                promise.setFailure(error);
            }
        });
    }

    @Override
    public CompletionStage<V> exceptionally(Function<Throwable, ? extends V> fn) {
        requireNonNull(fn, "fn");

        Promise<V> promise = executor().newPromise();
        future.addListener(f -> {
            Throwable error = f.cause();
            if (error == null) {
                @SuppressWarnings("unchecked") V value = (V) f.getNow();
                promise.setSuccess(value);
            } else {
                final V result;
                try {
                    result = fn.apply(error);
                } catch (Throwable cause) {
                    promise.setFailure(cause);
                    return;
                }
                promise.setSuccess(result);
            }
        });
        return promise.asStage();
    }

    @Override
    public CompletionStage<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action, Executor executor) {
        requireNonNull(action, "action");
        requireNonNull(executor, "executor");

        Promise<V> promise = executor().newPromise();
        future.addListener(f -> {
            if (executeDirectly(executor)) {
                whenCompleteAsync0(promise, f, action);
            } else {
                safeExecute(executor, () -> whenCompleteAsync0(promise, f, action), promise);
            }
        });
        return promise.asStage();
    }

    @SuppressWarnings("unchecked")
    private static <U, V> void whenCompleteAsync0(
            Promise<U> promise, Future<? super V> f, BiConsumer<? super V, ? super Throwable> action) {
        try {
            action.accept((V) f.getNow(), f.cause());
        } catch (Throwable cause) {
            promise.setFailure(cause);
            return;
        }
        promise.setSuccess(null);
    }

    @Override
    public <U> CompletionStage<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        requireNonNull(fn, "fn");
        requireNonNull(executor, "executor");

        Promise<U> promise = executor().newPromise();
        future.addListener(f -> {
            if (executeDirectly(executor)) {
                handleAsync0(promise, f, fn);
            } else {
                safeExecute(executor, () -> handleAsync0(promise, f, fn), promise);
            }
        });
        return promise.asStage();
    }

    @SuppressWarnings("unchecked")
    private static <U, V> void handleAsync0(
            Promise<U> promise, Future<? super V> f, BiFunction<? super V, Throwable, ? extends U> fn) {
        Throwable cause = f.cause();
        applyAndNotify(promise, cause == null ? (V) f.getNow() : null, cause, fn);
    }

    private static <U, V, T> void applyAndNotify(
            Promise<U> promise, V value, T value2, BiFunction<? super V, ? super T, ? extends U> fn) {
        final U result;
        try {
            result = fn.apply(value, value2);
        } catch (Throwable error) {
            promise.setFailure(error);
            return;
        }
        promise.setSuccess(result);
    }

    private static boolean executeDirectly(Executor executor) {
        return executor == ImmediateExecutor.INSTANCE || executor == ImmediateEventExecutor.INSTANCE;
    }

    private static void safeExecute(Executor executor, Runnable task, Promise<?> promise) {
        try {
            executor.execute(task);
        } catch (Throwable cause) {
            promise.setFailure(cause);
        }
    }

    private abstract static class AtomicBiConsumer<V, U> extends AtomicReference<Object>
            implements BiConsumer<V, Throwable> {

        private final Promise<U> promise;

        AtomicBiConsumer(Promise<U> promise) {
            super(Marker.EMPTY);
            this.promise = promise;
        }

        @Override
        public void accept(V v, Throwable error) {
            if (error == null) {
                if (compareAndSet(Marker.EMPTY, v)) {
                    final U value;
                    try {
                        value = apply(v);
                    } catch (Throwable cause) {
                        promise.setFailure(cause);
                        return;
                    }
                    promise.setSuccess(value);
                }
            } else if (compareAndSet(Marker.EMPTY, Marker.ERROR)) {
                promise.setFailure(error);
            }
        }

        protected abstract U apply(V value);
    }
}
