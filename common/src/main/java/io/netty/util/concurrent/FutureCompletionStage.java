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

import io.netty.util.internal.StringUtil;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link CompletionStage} that provides the same threading semantics and guarantees as the underlying
 * {@link io.netty.util.concurrent.Future}, which means that all the callbacks will be executed by {@link #executor()}
 * if not specified otherwise (by calling the corresponding *Async methods).
 *
 * Please be aware that {@link FutureCompletionStage#toCompletableFuture()} is not supported and so will throw
 * a {@link UnsupportedOperationException} when invoked.
 *
 * @param <V> the value type.
 */
public interface FutureCompletionStage<V> extends CompletionStage<V> {

    /**
     * Returns the underlying {@link Future} of this {@link FutureCompletionStage}.
     */
    Future<V> future();

    /**
     * See {@link Future#executor()}.
     */
    default EventExecutor executor() {
        return future().executor();
    }

    /**
     * Not supported and so throws an {@link UnsupportedOperationException}.
     */
    @Override
    default CompletableFuture<V> toCompletableFuture() {
        throw new UnsupportedOperationException("Not supported by "
                + StringUtil.simpleClassName(FutureCompletionStage.class));
    }

    @Override
    default  <U> CompletionStage<U> thenApply(Function<? super V, ? extends U> fn) {
        return thenApplyAsync(fn, ImmediateExecutor.INSTANCE);
    }

    @Override
    default <U> CompletionStage<U> thenApplyAsync(Function<? super V, ? extends U> fn) {
        return thenApplyAsync(fn, ForkJoinPool.commonPool());
    }

    @Override
    default CompletionStage<Void> thenAccept(Consumer<? super V> action) {
        return thenAcceptAsync(action, ImmediateExecutor.INSTANCE);
    }

    @Override
    default CompletionStage<Void> thenAcceptAsync(Consumer<? super V> action) {
        return thenAcceptAsync(action, ForkJoinPool.commonPool());
    }

    @Override
    default CompletionStage<Void> thenRun(Runnable action) {
        return thenRunAsync(action, ImmediateExecutor.INSTANCE);
    }

    @Override
    default CompletionStage<Void> thenRunAsync(Runnable action) {
        return thenRunAsync(action, ForkJoinPool.commonPool());
    }

    @Override
    default  <U, V1> CompletionStage<V1> thenCombine(
            CompletionStage<? extends U> other, BiFunction<? super V, ? super U, ? extends V1> fn) {
        return thenCombineAsync(other, fn, ImmediateExecutor.INSTANCE);
    }

    @Override
    default <U, V1> CompletionStage<V1> thenCombineAsync(
            CompletionStage<? extends U> other, BiFunction<? super V, ? super U, ? extends V1> fn) {
        return thenCombineAsync(other, fn, ForkJoinPool.commonPool());
    }

    @Override
    default  <U> CompletionStage<Void> thenAcceptBoth(
            CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action) {
        return thenAcceptBothAsync(other, action, ImmediateExecutor.INSTANCE);
    }

    @Override
    default <U> CompletionStage<Void> thenAcceptBothAsync(
            CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action) {
        return thenAcceptBothAsync(other, action, ForkJoinPool.commonPool());
    }

    @Override
    default CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return runAfterBothAsync(other, action, ImmediateExecutor.INSTANCE);
    }

    @Override
    default CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return runAfterBothAsync(other, action, ForkJoinPool.commonPool());
    }

    @Override
    default  <U> CompletionStage<U> applyToEither(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return applyToEitherAsync(other, fn, ImmediateExecutor.INSTANCE);
    }

    @Override
    default <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return applyToEitherAsync(other, fn, ForkJoinPool.commonPool());
    }

    @Override
    default CompletionStage<Void> acceptEither(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return acceptEitherAsync(other, action, ImmediateExecutor.INSTANCE);
    }

    @Override
    default CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return acceptEitherAsync(other, action, ForkJoinPool.commonPool());
    }

    @Override
    default CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return runAfterEitherAsync(other, action, ImmediateExecutor.INSTANCE);
    }

    @Override
    default CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return runAfterEitherAsync(other, action, ForkJoinPool.commonPool());
    }

    @Override
    default <U> CompletionStage<U> thenCompose(Function<? super V, ? extends CompletionStage<U>> fn) {
        return thenComposeAsync(fn, ImmediateExecutor.INSTANCE);
    }

    @Override
    default <U> CompletionStage<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn) {
        return thenComposeAsync(fn, ForkJoinPool.commonPool());
    }

    @Override
    default CompletionStage<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
        return whenCompleteAsync(action, ImmediateExecutor.INSTANCE);
    }

    @Override
    default CompletionStage<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action) {
        return whenCompleteAsync(action, ForkJoinPool.commonPool());
    }

    @Override
    default <U> CompletionStage<U> handle(BiFunction<? super V, Throwable, ? extends U> fn) {
        return handleAsync(fn, ImmediateExecutor.INSTANCE);
    }

    @Override
    default <U> CompletionStage<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn) {
        return handleAsync(fn, ForkJoinPool.commonPool());
    }
}
