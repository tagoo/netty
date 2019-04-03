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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DefaultFutureCompletionStageTest {

    private static EventExecutorGroup group;
    private static EventExecutorGroup asyncExecutorGroup;

    @BeforeClass
    public static void setup() {
        group = new MultithreadEventExecutorGroup(1, Executors.defaultThreadFactory());
        asyncExecutorGroup = new MultithreadEventExecutorGroup(1, Executors.defaultThreadFactory());
    }

    @AfterClass
    public static void destroy() {
        group.shutdownGracefully();
        asyncExecutorGroup.shutdownGracefully();
    }

    private static EventExecutor executor() {
        return group.next();
    }

    private static EventExecutor asyncExecutor() {
        return asyncExecutorGroup.next();
    }

    @Test
    public void testSameExecutorAndFuture() {
        EventExecutor executor = executor();
        Promise<Boolean> promise = executor.newPromise();
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(promise);
        assertSame(executor, stage.executor());
        assertSame(promise, stage.future());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testThrowsUnsupportedOperationException() {
        EventExecutor executor = executor();
        Promise<Boolean> promise = executor.newPromise();
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(promise);
        stage.toCompletableFuture();
    }

    @Test
    public void testThenApply() {
        testThenApply0(stage -> stage.thenApply(v -> {
            assertSame(Boolean.TRUE, v);
            assertTrue(stage.executor().inEventLoop());

            return Boolean.FALSE;
        }));
    }

    @Test
    public void testThenApplyAsync() {
        testThenApply0(stage -> stage.thenApplyAsync(v -> {
            assertSame(Boolean.TRUE, v);
            assertFalse(stage.executor().inEventLoop());

            return Boolean.FALSE;
        }));
    }

    @Test
    public void testThenApplyAsyncWithExecutor() {
        EventExecutor asyncExecutor = asyncExecutor();
        testThenApply0(stage -> stage.thenApplyAsync(v -> {
            assertSame(Boolean.TRUE, v);
            assertFalse(stage.executor().inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());

            return Boolean.FALSE;
        }, asyncExecutor));
    }

    private void testThenApply0(Function<FutureCompletionStage<Boolean>, FutureCompletionStage<Boolean>> fn) {
        EventExecutor executor = executor();
        Future<Boolean> future = executor.newSucceededFuture(Boolean.TRUE);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        assertSame(Boolean.FALSE, fn.apply(stage).future().syncUninterruptibly().getNow());
    }

    @Test
    public void testThenApplyFunctionThrows() {
        testThenApplyFunctionThrows((stage, exception) -> stage.thenApply(v -> {
            throw exception;
        }));
    }

    @Test
    public void testThenApplyAsyncFunctionThrows() {
        testThenApplyFunctionThrows((stage, exception) -> stage.thenApplyAsync(v -> {
            throw exception;
        }));
    }

    @Test
    public void testThenApplyAsyncWithExecutorFunctionThrows() {
        EventExecutor asyncExecutor = asyncExecutor();
        testThenApplyFunctionThrows((stage, exception) -> stage.thenApplyAsync(v -> {
            throw exception;
        }, asyncExecutor));
    }

    private void testThenApplyFunctionThrows(
            BiFunction<FutureCompletionStage<Boolean>, IllegalStateException, FutureCompletionStage<Boolean>> fn) {
        IllegalStateException exception = new IllegalStateException();
        EventExecutor executor = executor();
        Future<Boolean> future = executor.newSucceededFuture(Boolean.TRUE);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        try {
            fn.apply(stage, exception).future().syncUninterruptibly();
        } catch (IllegalStateException e) {
            assertSame(exception, e);
        }
    }

    @Test
    public void testThenAccept() {
        testThenAccept0(stage -> stage.thenAccept(v -> {
            assertSame(Boolean.TRUE, v);
            assertTrue(stage.executor().inEventLoop());
        }));
    }

    @Test
    public void testThenAcceptAsync() {
        testThenAccept0(stage -> stage.thenAcceptAsync(v -> {
            assertSame(Boolean.TRUE, v);

            assertFalse(stage.executor().inEventLoop());
        }));
    }

    @Test
    public void testThenAcceptAsyncWithExecutor() {
        EventExecutor asyncExecutor = asyncExecutor();
        testThenAccept0(stage -> stage.thenAcceptAsync(v -> {
            assertSame(Boolean.TRUE, v);

            assertFalse(stage.executor().inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());
        }, asyncExecutor));
    }

    private void testThenAccept0(Function<FutureCompletionStage<Boolean>, FutureCompletionStage<Void>> fn) {
        EventExecutor executor = executor();
        Future<Boolean> future = executor.newSucceededFuture(Boolean.TRUE);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        assertNull(fn.apply(stage).future().syncUninterruptibly().getNow());
    }

    @Test
    public void testThenAcceptConsumerThrows() {
        testThenAcceptConsumerThrows0((stage, e) -> stage.thenAccept(v -> {
            throw e;
        }));
    }

    @Test
    public void testThenAcceptAsyncConsumerThrows() {
        testThenAcceptConsumerThrows0((stage, e) -> stage.thenAcceptAsync(v -> {
            throw e;
        }));
    }

    @Test
    public void testThenAcceptAsyncWithExecutorConsumerThrows() {
        EventExecutor asyncExecutor = asyncExecutor();
        testThenAcceptConsumerThrows0((stage, e) -> stage.thenAcceptAsync(v -> {
            throw e;
        }, asyncExecutor));
    }

    private void testThenAcceptConsumerThrows0(
            BiFunction<FutureCompletionStage<Boolean>, IllegalStateException, FutureCompletionStage<Void>> fn) {
        IllegalStateException exception = new IllegalStateException();
        EventExecutor executor = executor();
        Future<Boolean> future = executor.newSucceededFuture(Boolean.TRUE);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        try {
            fn.apply(stage, exception).future().syncUninterruptibly();
        } catch (IllegalStateException e) {
            assertSame(exception, e);
        }
    }

    @Test
    public void testThenRun() {
        testThenAccept0(stage -> stage.thenRun(() -> {
            assertTrue(stage.executor().inEventLoop());
        }));
    }

    @Test
    public void testThenRunAsync() {
        testThenAccept0(stage -> stage.thenRunAsync(() -> {
            assertFalse(stage.executor().inEventLoop());
        }));
    }

    @Test
    public void testThenRunAsyncWithExecutor() {
        EventExecutor asyncExecutor = asyncExecutor();
        testThenAccept0(stage -> stage.thenRunAsync(() -> {
            assertFalse(stage.executor().inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());
        }, asyncExecutor));
    }

    @Test
    public void testThenRunTaskThrows() {
        testThenAcceptConsumerThrows0((stage, e) -> stage.thenRun(() -> {
            throw e;
        }));
    }

    @Test
    public void testThenRunAsyncTaskThrows() {
        testThenAcceptConsumerThrows0((stage, e) -> stage.thenRunAsync(() -> {
            throw e;
        }));
    }

    @Test
    public void testThenRunAsyncWithExecutorTaskThrows() {
        EventExecutor asyncExecutor = asyncExecutor();
        testThenAcceptConsumerThrows0((stage, e) -> stage.thenRunAsync(() -> {
            throw e;
        }, asyncExecutor));
    }

    @Test
    public void testThenCombine() {
        EventExecutor executor = executor();
        String expected = "test";
        for (int i = 0; i < 1000; i++) {
            Promise<Boolean> promise = executor.newPromise();
            FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(promise);
            CompletableFuture<String> completableFuture = new CompletableFuture<>();

            Future<Integer> f = stage.thenCombine(completableFuture, (v1, v2) -> {
                assertSame(v1, Boolean.TRUE);
                assertSame(v2, expected);
                assertTrue(executor.inEventLoop());

                return 1;
            }).future();

            List<Runnable> runnables = new ArrayList<>();
            Collections.addAll(runnables, () -> completableFuture.complete(expected),
                    () -> promise.setSuccess(Boolean.TRUE));
            Collections.shuffle(runnables);

            for (Runnable task : runnables) {
                ForkJoinPool.commonPool().execute(task);
            }
            assertEquals(1, f.syncUninterruptibly().getNow().intValue());
        }
    }

    @Test
    public void testThenCombineAsync() {
        EventExecutor executor = executor();
        String expected = "test";
        for (int i = 0; i < 1000; i++) {
            Promise<Boolean> promise = executor.newPromise();
            FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(promise);
            CompletableFuture<String> completableFuture = new CompletableFuture<>();

            Future<Integer> f = stage.thenCombineAsync(completableFuture, (v1, v2) -> {
                assertSame(v1, Boolean.TRUE);
                assertSame(v2, expected);
                assertFalse(executor.inEventLoop());
                return 1;
            }).future();

            List<Runnable> runnables = new ArrayList<>();
            Collections.addAll(runnables, () -> completableFuture.complete(expected),
                    () -> promise.setSuccess(Boolean.TRUE));
            Collections.shuffle(runnables);

            for (Runnable task : runnables) {
                ForkJoinPool.commonPool().execute(task);
            }
            assertEquals(1, f.syncUninterruptibly().getNow().intValue());
        }
    }

    @Test
    public void testThenCombineAsyncWithExecutor() {
        EventExecutor executor = executor();
        EventExecutor asyncExecutor = asyncExecutor();
        String expected = "test";
        for (int i = 0; i < 1000; i++) {
            Promise<Boolean> promise = executor.newPromise();
            FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(promise);
            CompletableFuture<String> completableFuture = new CompletableFuture<>();

            Future<Integer> f = stage.thenCombineAsync(completableFuture, (v1, v2) -> {
                assertSame(v1, Boolean.TRUE);
                assertSame(v2, expected);
                assertFalse(executor.inEventLoop());
                return 1;
            }, asyncExecutor).future();

            List<Runnable> runnables = new ArrayList<>();
            Collections.addAll(runnables, () -> completableFuture.complete(expected),
                    () -> promise.setSuccess(Boolean.TRUE));
            Collections.shuffle(runnables);

            for (Runnable task : runnables) {
                ForkJoinPool.commonPool().execute(task);
            }
            assertEquals(1, f.syncUninterruptibly().getNow().intValue());
        }
    }

    @Test
    public void testThenCombineThrowable() {
        IllegalStateException exception = new IllegalStateException();

        EventExecutor executor = executor();
        for (int i = 0; i < 1000; i++) {
            Promise<Boolean> promise = executor.newPromise();
            FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(promise);
            CompletableFuture<String> completableFuture = new CompletableFuture<>();

            Future<?> f = stage.thenCombineAsync(completableFuture, (v1, v2) -> {
                fail();
                return null;
            }).future();

            List<Runnable> runnables = new ArrayList<>();
            Collections.addAll(runnables, () -> completableFuture.completeExceptionally(exception),
                    () -> promise.tryFailure(exception));
            Collections.shuffle(runnables);

            for (Runnable task : runnables) {
                ForkJoinPool.commonPool().execute(task);
            }
            assertSame(exception, f.awaitUninterruptibly().cause());
        }
    }

    @Test
    public void testThenCombineAsyncThrowable() {
        IllegalStateException exception = new IllegalStateException();

        EventExecutor executor = executor();
        EventExecutor asyncExecutor = asyncExecutor();
        for (int i = 0; i < 1000; i++) {
            Promise<Boolean> promise = executor.newPromise();
            FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(promise);
            CompletableFuture<String> completableFuture = new CompletableFuture<>();

            Future<?> f = stage.thenCombineAsync(completableFuture, (v1, v2) -> {
                fail();
                return null;
            }, asyncExecutor).future();

            List<Runnable> runnables = new ArrayList<>();
            Collections.addAll(runnables, () -> completableFuture.completeExceptionally(exception),
                    () -> promise.tryFailure(exception));
            Collections.shuffle(runnables);

            for (Runnable task : runnables) {
                ForkJoinPool.commonPool().execute(task);
            }
            assertSame(exception, f.awaitUninterruptibly().cause());
        }
    }

    /*
    @Test
    public void testHandleAsyncWithExecutorThrowable() {
        IllegalStateException exception = new IllegalStateException();
        EventExecutor executor = executor();
        EventExecutor asyncExecutor = asyncExecutor();
        Future<Boolean> future = executor.newFailedFuture(exception);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        assertSame(Boolean.FALSE, stage.handleAsync((v, cause) -> {
            assertSame(exception, cause);
            assertNull(v);

            assertFalse(executor.inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());
            return Boolean.FALSE;
        }, asyncExecutor).future().syncUninterruptibly().getNow());
    }

    @Test
    public void testHandleFunctionThrows() {
        IllegalStateException exception = new IllegalStateException();
        EventExecutor executor = executor();
        Future<Boolean> future = executor.newSucceededFuture(Boolean.TRUE);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        try {
            stage.handle((v, cause) -> {
                throw exception;
            }).future().syncUninterruptibly();
        } catch (IllegalStateException e) {
            assertSame(exception, e);
        }
    }

    @Test
    public void testHandleAsyncFunctionThrows() {
        IllegalStateException exception = new IllegalStateException();
        EventExecutor executor = executor();
        Future<Boolean> future = executor.newSucceededFuture(Boolean.TRUE);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        try {
            stage.handleAsync((v, cause) -> {
                throw exception;
            }).future().syncUninterruptibly();
        } catch (IllegalStateException e) {
            assertSame(exception, e);
        }
    }

    @Test
    public void testHandleAsyncWithExecutorFunctionThrows() {
        IllegalStateException exception = new IllegalStateException();
        EventExecutor executor = executor();
        EventExecutor asyncExecutor = asyncExecutor();
        Future<Boolean> future = executor.newSucceededFuture(Boolean.TRUE);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        try {
            stage.handleAsync((v, cause) -> {
                throw exception;
            }, asyncExecutor).future().syncUninterruptibly();
        } catch (IllegalStateException e) {
            assertSame(exception, e);
        }
    }
    */

    @Test
    public void testHandleAsync() {
        EventExecutor executor = executor();
        Future<Boolean> future = executor.newSucceededFuture(Boolean.TRUE);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        assertSame(Boolean.FALSE, stage.handleAsync((v, cause) -> {
            assertSame(Boolean.TRUE, v);
            assertNull(cause);

            assertFalse(executor.inEventLoop());

            return Boolean.FALSE;
        }).future().syncUninterruptibly().getNow());
    }

    @Test
    public void testHandleAsyncWithExecutor() {
        EventExecutor executor = executor();
        EventExecutor asyncExecutor = asyncExecutor();
        Future<Boolean> future = executor.newSucceededFuture(Boolean.TRUE);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        assertSame(Boolean.FALSE, stage.handleAsync((v, cause) -> {
            assertSame(Boolean.TRUE, v);
            assertNull(cause);

            assertFalse(executor.inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());
            return Boolean.FALSE;
        }, asyncExecutor).future().syncUninterruptibly().getNow());
    }

    @Test
    public void testHandleThrowable() {
        IllegalStateException exception = new IllegalStateException();
        EventExecutor executor = executor();
        Future<Boolean> future = executor.newFailedFuture(exception);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        assertSame(Boolean.FALSE, stage.handle((v, cause) -> {
            assertSame(exception, cause);
            assertNull(v);

            assertTrue(executor.inEventLoop());

            return Boolean.FALSE;
        }).future().syncUninterruptibly().getNow());
    }

    @Test
    public void testHandleAsyncThrowable() {
        IllegalStateException exception = new IllegalStateException();
        EventExecutor executor = executor();
        Future<Boolean> future = executor.newFailedFuture(exception);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        assertSame(Boolean.FALSE, stage.handleAsync((v, cause) -> {
            assertSame(exception, cause);
            assertNull(v);

            assertFalse(executor.inEventLoop());

            return Boolean.FALSE;
        }).future().syncUninterruptibly().getNow());
    }

    @Test
    public void testHandleAsyncWithExecutorThrowable() {
        IllegalStateException exception = new IllegalStateException();
        EventExecutor executor = executor();
        EventExecutor asyncExecutor = asyncExecutor();
        Future<Boolean> future = executor.newFailedFuture(exception);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        assertSame(Boolean.FALSE, stage.handleAsync((v, cause) -> {
            assertSame(exception, cause);
            assertNull(v);

            assertFalse(executor.inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());
            return Boolean.FALSE;
        }, asyncExecutor).future().syncUninterruptibly().getNow());
    }

    @Test
    public void testHandleFunctionThrows() {
        IllegalStateException exception = new IllegalStateException();
        EventExecutor executor = executor();
        Future<Boolean> future = executor.newSucceededFuture(Boolean.TRUE);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        try {
            stage.handle((v, cause) -> {
                throw exception;
            }).future().syncUninterruptibly();
        } catch (IllegalStateException e) {
            assertSame(exception, e);
        }
    }

    @Test
    public void testHandleAsyncFunctionThrows() {
        IllegalStateException exception = new IllegalStateException();
        EventExecutor executor = executor();
        Future<Boolean> future = executor.newSucceededFuture(Boolean.TRUE);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        try {
            stage.handleAsync((v, cause) -> {
                throw exception;
            }).future().syncUninterruptibly();
        } catch (IllegalStateException e) {
            assertSame(exception, e);
        }
    }

    @Test
    public void testHandleAsyncWithExecutorFunctionThrows() {
        IllegalStateException exception = new IllegalStateException();
        EventExecutor executor = executor();
        EventExecutor asyncExecutor = asyncExecutor();
        Future<Boolean> future = executor.newSucceededFuture(Boolean.TRUE);
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        try {
            stage.handleAsync((v, cause) -> {
                throw exception;
            }, asyncExecutor).future().syncUninterruptibly();
        } catch (IllegalStateException e) {
            assertSame(exception, e);
        }
    }
}
