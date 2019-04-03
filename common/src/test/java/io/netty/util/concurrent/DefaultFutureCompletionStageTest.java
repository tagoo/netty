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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
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
    private static final IllegalStateException EXCEPTION = new IllegalStateException();
    private static final String EXPECTED_STRING = "test";

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

    private static Future<Boolean> newSucceededFuture() {
        return executor().newSucceededFuture(Boolean.TRUE);
    }

    private static <T> Future<T> newFailedFuture() {
        return executor().newFailedFuture(EXCEPTION);
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
        }), false);
    }

    @Test
    public void testThenApplyAsync() {
        testThenApply0(stage -> stage.thenApplyAsync(v -> {
            assertSame(Boolean.TRUE, v);
            assertFalse(stage.executor().inEventLoop());

            return Boolean.FALSE;
        }), false);
    }

    @Test
    public void testThenApplyAsyncWithExecutor() {
        EventExecutor asyncExecutor = asyncExecutor();
        testThenApply0(stage -> stage.thenApplyAsync(v -> {
            assertSame(Boolean.TRUE, v);
            assertFalse(stage.executor().inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());

            return Boolean.FALSE;
        }, asyncExecutor), false);
    }

    @Test
    public void testThenApplyFunctionThrows() {
        testThenApply0(stage -> stage.thenApply(v -> {
            throw EXCEPTION;
        }), true);
    }

    @Test
    public void testThenApplyAsyncFunctionThrows() {
        testThenApply0(stage -> stage.thenApplyAsync(v -> {
            throw EXCEPTION;
        }), true);
    }

    @Test
    public void testThenApplyAsyncWithExecutorFunctionThrows() {
        EventExecutor asyncExecutor = asyncExecutor();
        testThenApply0(stage -> stage.thenApplyAsync(v -> {
            throw EXCEPTION;
        }, asyncExecutor), true);
    }

    private void testHandle0(Future<Boolean> future,
                             Function<FutureCompletionStage<Boolean>, FutureCompletionStage<Boolean>> fn,
                             boolean exception) {
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);

        Future<Boolean> f = fn.apply(stage).future().awaitUninterruptibly();
        if (exception) {
            assertSame(EXCEPTION, f.cause());
        } else {
            assertSame(Boolean.FALSE, f.syncUninterruptibly().getNow());
        }
    }

    private void testThenApply0(
            Function<FutureCompletionStage<Boolean>, FutureCompletionStage<Boolean>> fn, boolean exception) {
        testHandle0(newSucceededFuture(), fn, exception);
    }

    @Test
    public void testThenAccept() {
        testThenAccept0(stage -> stage.thenAccept(v -> {
            assertSame(Boolean.TRUE, v);
            assertTrue(stage.executor().inEventLoop());
        }), false);
    }

    @Test
    public void testThenAcceptAsync() {
        testThenAccept0(stage -> stage.thenAcceptAsync(v -> {
            assertSame(Boolean.TRUE, v);

            assertFalse(stage.executor().inEventLoop());
        }), false);
    }

    @Test
    public void testThenAcceptAsyncWithExecutor() {
        EventExecutor asyncExecutor = asyncExecutor();
        testThenAccept0(stage -> stage.thenAcceptAsync(v -> {
            assertSame(Boolean.TRUE, v);

            assertFalse(stage.executor().inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());
        }, asyncExecutor), false);
    }

    @Test
    public void testThenAcceptConsumerThrows() {
        testThenAccept0(stage -> stage.thenAccept(v -> {
            throw EXCEPTION;
        }), true);
    }

    @Test
    public void testThenAcceptAsyncConsumerThrows() {
        testThenAccept0(stage -> stage.thenAcceptAsync(v -> {
            throw EXCEPTION;
        }), true);
    }

    @Test
    public void testThenAcceptAsyncWithExecutorConsumerThrows() {
        EventExecutor asyncExecutor = asyncExecutor();
        testThenAccept0(stage -> stage.thenAcceptAsync(v -> {
            throw EXCEPTION;
        }, asyncExecutor), true);
    }

    private void testThenAccept0(
            Function<FutureCompletionStage<Boolean>, FutureCompletionStage<Void>> fn, boolean exception) {
        Future<Boolean> future = newSucceededFuture();
        FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(future);
        Future<Void> f = fn.apply(stage).future().awaitUninterruptibly();
        if (exception) {
            assertSame(EXCEPTION, f.cause());
        } else {
            assertNull(f.syncUninterruptibly().getNow());
        }
    }

    @Test
    public void testThenRun() {
        testThenAccept0(stage -> stage.thenRun(() -> {
            assertTrue(stage.executor().inEventLoop());
        }), false);
    }

    @Test
    public void testThenRunAsync() {
        testThenAccept0(stage -> stage.thenRunAsync(() -> {
            assertFalse(stage.executor().inEventLoop());
        }), false);
    }

    @Test
    public void testThenRunAsyncWithExecutor() {
        EventExecutor asyncExecutor = asyncExecutor();
        testThenAccept0(stage -> stage.thenRunAsync(() -> {
            assertFalse(stage.executor().inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());
        }, asyncExecutor), false);
    }

    @Test
    public void testThenRunTaskThrows() {
        testThenAccept0(stage -> stage.thenRun(() -> {
            throw EXCEPTION;
        }), true);
    }

    @Test
    public void testThenRunAsyncTaskThrows() {
        testThenAccept0(stage -> stage.thenRunAsync(() -> {
            throw EXCEPTION;
        }), true);
    }

    @Test
    public void testThenRunAsyncWithExecutorTaskThrows() {
        EventExecutor asyncExecutor = asyncExecutor();
        testThenAccept0(stage -> stage.thenRunAsync(() -> {
            throw EXCEPTION;
        }, asyncExecutor), true);
    }

    @Test
    public void testThenCombine() {
        testThenCombine0((stage, other) -> stage.thenCombine(other, (v1, v2) -> {
            assertSame(v1, Boolean.TRUE);
            assertSame(v2, EXPECTED_STRING);
            assertTrue(stage.executor().inEventLoop());

            return 1;
        }), CombineAndAcceptTestMode.COMPLETE);
    }

    @Test
    public void testThenCombineAsync() {
        testThenCombine0((stage, other) -> stage.thenCombineAsync(other, (v1, v2) -> {
            assertSame(v1, Boolean.TRUE);
            assertSame(v2, EXPECTED_STRING);
            assertFalse(stage.executor().inEventLoop());

            return 1;
        }), CombineAndAcceptTestMode.COMPLETE);
    }

    @Test
    public void testThenCombineAsyncWithExecutor() {
        EventExecutor asyncExecutor = asyncExecutor();

        testThenCombine0((stage, other) -> stage.thenCombineAsync(other, (v1, v2) -> {
            assertSame(v1, Boolean.TRUE);
            assertSame(v2, EXPECTED_STRING);
            assertFalse(stage.executor().inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());
            return 1;
        }, asyncExecutor), CombineAndAcceptTestMode.COMPLETE);
    }

    @Test
    public void testThenCombineThrowable() {
        testThenCombine0((stage, other) -> stage.thenCombine(other, (v1, v2) -> {
            fail();
            return 1;
        }), CombineAndAcceptTestMode.COMPLETE_EXCEPTIONAL);
    }

    @Test
    public void testThenCombineAsyncThrowable() {
        testThenCombine0((stage, other) -> stage.thenCombineAsync(other, (v1, v2) -> {
            fail();
            return 1;
        }), CombineAndAcceptTestMode.COMPLETE_EXCEPTIONAL);
    }

    @Test
    public void testThenCombineAsyncWithExecutorThrowable() {
        EventExecutor asyncExecutor = asyncExecutor();

        testThenCombine0((stage, other) -> stage.thenCombineAsync(other, (v1, v2) -> {
            fail();
            return 1;
        }, asyncExecutor), CombineAndAcceptTestMode.COMPLETE_EXCEPTIONAL);
    }

    @Test
    public void testThenCombineThrows() {
        testThenCombine0((stage, other) -> stage.thenCombine(other, (v1, v2) -> {
            throw EXCEPTION;
        }), CombineAndAcceptTestMode.THROW);
    }

    @Test
    public void testThenCombineAsyncThrows() {
        testThenCombine0((stage, other) -> stage.thenCombineAsync(other, (v1, v2) -> {
            throw EXCEPTION;
        }), CombineAndAcceptTestMode.THROW);
    }

    @Test
    public void testThenCombineAsyncWithExecutorThrows() {
        EventExecutor asyncExecutor = asyncExecutor();

        testThenCombine0((stage, other) -> stage.thenCombineAsync(other, (v1, v2) -> {
            throw EXCEPTION;
        }, asyncExecutor), CombineAndAcceptTestMode.THROW);
    }

    @Test
    public void testThenAcceptBoth() {
        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.thenAcceptBoth(other, (v1, v2) -> {
            assertSame(v1, Boolean.TRUE);
            assertSame(v2, EXPECTED_STRING);
            assertTrue(stage.executor().inEventLoop());
        }), CombineAndAcceptTestMode.COMPLETE);
    }

    @Test
    public void testThenAcceptBothAsync() {
        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.thenAcceptBothAsync(other, (v1, v2) -> {
            assertSame(v1, Boolean.TRUE);
            assertSame(v2, EXPECTED_STRING);
            assertFalse(stage.executor().inEventLoop());
        }), CombineAndAcceptTestMode.COMPLETE);
    }

    @Test
    public void testThenAcceptBothAsyncWithExecutor() {
        EventExecutor asyncExecutor = asyncExecutor();

        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.thenAcceptBothAsync(other, (v1, v2) -> {
            assertSame(v1, Boolean.TRUE);
            assertSame(v2, EXPECTED_STRING);
            assertFalse(stage.executor().inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());
        }, asyncExecutor), CombineAndAcceptTestMode.COMPLETE);
    }

    @Test
    public void testThenAcceptBothThrowable() {
        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.thenAcceptBoth(other, (v1, v2) -> {
            fail();
        }), CombineAndAcceptTestMode.COMPLETE_EXCEPTIONAL);
    }

    @Test
    public void testThenAcceptBothAsyncThrowable() {
        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.thenAcceptBothAsync(other, (v1, v2) -> {
            fail();
        }), CombineAndAcceptTestMode.COMPLETE_EXCEPTIONAL);
    }

    @Test
    public void testThenAcceptBothAsyncWithExecutorThrowable() {
        EventExecutor asyncExecutor = asyncExecutor();

        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.thenAcceptBothAsync(other, (v1, v2) -> {
            fail();
        }, asyncExecutor), CombineAndAcceptTestMode.COMPLETE_EXCEPTIONAL);
    }

    @Test
    public void testThenAcceptBothThrows() {
        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.thenAcceptBoth(other, (v1, v2) -> {
            throw EXCEPTION;
        }), CombineAndAcceptTestMode.THROW);
    }

    @Test
    public void testThenAcceptBothAsyncThrows() {
        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.thenCombineAsync(other, (v1, v2) -> {
            throw EXCEPTION;
        }), CombineAndAcceptTestMode.THROW);
    }

    @Test
    public void testThenAcceptBothAsyncWithExecutorThrows() {
        EventExecutor asyncExecutor = asyncExecutor();

        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.thenCombineAsync(other, (v1, v2) -> {
            throw EXCEPTION;
        }, asyncExecutor), CombineAndAcceptTestMode.THROW);
    }

    @Test
    public void testRunAfterBoth() {
        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.runAfterBoth(other, () -> {
            assertTrue(stage.executor().inEventLoop());
        }), CombineAndAcceptTestMode.COMPLETE);
    }

    @Test
    public void testRunAfterBothAsync() {
        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.runAfterBothAsync(other, () -> {
            assertFalse(stage.executor().inEventLoop());
        }), CombineAndAcceptTestMode.COMPLETE);
    }

    @Test
    public void testRunAfterBothAsyncWithExecutor() {
        EventExecutor asyncExecutor = asyncExecutor();

        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.runAfterBothAsync(other, () -> {
            assertFalse(stage.executor().inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());
        }, asyncExecutor), CombineAndAcceptTestMode.COMPLETE);
    }

    @Test
    public void testRunAfterBothThrowable() {
        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.runAfterBothAsync(other, () -> {
            fail();
        }), CombineAndAcceptTestMode.COMPLETE_EXCEPTIONAL);
    }

    @Test
    public void testRunAfterBothAsyncThrowable() {
        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.runAfterBothAsync(other, () -> {
            fail();
        }), CombineAndAcceptTestMode.COMPLETE_EXCEPTIONAL);
    }

    @Test
    public void testRunAfterBothAsyncWithExecutorThrowable() {
        EventExecutor asyncExecutor = asyncExecutor();

        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.runAfterBothAsync(other, () -> {
            fail();
        }, asyncExecutor), CombineAndAcceptTestMode.COMPLETE_EXCEPTIONAL);
    }

    @Test
    public void testRunAfterBothThrows() {
        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.runAfterBothAsync(other, () -> {
            throw EXCEPTION;
        }), CombineAndAcceptTestMode.THROW);
    }

    @Test
    public void testRunAfterBothAsyncThrows() {
        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.runAfterBothAsync(other, () -> {
            throw EXCEPTION;
        }), CombineAndAcceptTestMode.THROW);
    }

    @Test
    public void testRunAfterBothAsyncWithExecutorThrows() {
        EventExecutor asyncExecutor = asyncExecutor();

        testThenAcceptBothOrRunAfterBoth0((stage, other) -> stage.runAfterBothAsync(other, () -> {
            throw EXCEPTION;
        }, asyncExecutor), CombineAndAcceptTestMode.THROW);
    }

    private enum CombineAndAcceptTestMode {
        COMPLETE,
        COMPLETE_EXCEPTIONAL,
        THROW
    }

    private void testThenAcceptBothOrRunAfterBoth0(BiFunction<FutureCompletionStage<Boolean>,
            CompletionStage<String>, FutureCompletionStage<Void>> fn, CombineAndAcceptTestMode testMode) {
        testThenCombine0((futureStage, stage) -> fn.apply(futureStage, stage).thenApply(v -> 1), testMode);
    }

    private void testThenCombine0(BiFunction<FutureCompletionStage<Boolean>,
            CompletionStage<String>, FutureCompletionStage<Integer>> fn, CombineAndAcceptTestMode testMode) {
        EventExecutor executor = executor();
        for (int i = 0; i < 1000; i++) {
            Promise<Boolean> promise = executor.newPromise();
            FutureCompletionStage<Boolean> stage = new DefaultFutureCompletionStage<>(promise);
            CompletableFuture<String> completableFuture = new CompletableFuture<>();

            Future<Integer> f = fn.apply(stage, completableFuture).future();

            List<Runnable> runnables = new ArrayList<>(2);
            switch (testMode) {
                case THROW:
                    Collections.addAll(runnables, () -> completableFuture.completeExceptionally(EXCEPTION),
                            () -> promise.setFailure(EXCEPTION));
                    break;
                case COMPLETE_EXCEPTIONAL:
                    int random = ThreadLocalRandom.current().nextInt(0, 3);
                    if (random == 0) {
                        Collections.addAll(runnables, () -> completableFuture.complete(EXPECTED_STRING),
                                () -> promise.setFailure(EXCEPTION));
                    } else if (random == 1) {
                        Collections.addAll(runnables, () -> completableFuture.completeExceptionally(EXCEPTION),
                                () -> promise.setSuccess(Boolean.TRUE));
                    } else {
                        Collections.addAll(runnables, () -> completableFuture.completeExceptionally(EXCEPTION),
                                () -> promise.setFailure(EXCEPTION));
                    }
                    break;
                case COMPLETE:
                    Collections.addAll(runnables, () -> completableFuture.complete(EXPECTED_STRING),
                            () -> promise.setSuccess(Boolean.TRUE));
                    break;
                default:
                    fail();
            }

            Collections.shuffle(runnables);

            for (Runnable task : runnables) {
                ForkJoinPool.commonPool().execute(task);
            }

            f.awaitUninterruptibly();

            switch (testMode) {
                case COMPLETE_EXCEPTIONAL:
                case THROW:
                    assertSame(EXCEPTION, f.cause());
                    break;
                case COMPLETE:
                    assertEquals(1, f.syncUninterruptibly().getNow().intValue());
                    break;
                default:
                    fail();
            }
        }
    }

    @Test
    public void testHandle() {
        testHandle0(newSucceededFuture(), stage -> stage.handle((v, cause) -> {
            assertSame(Boolean.TRUE, v);
            assertNull(cause);

            assertTrue(stage.executor().inEventLoop());

            return Boolean.FALSE;
        }), false);
    }

    @Test
    public void testHandleAsync() {
        testHandle0(newSucceededFuture(), stage -> stage.handleAsync((v, cause) -> {
            assertSame(Boolean.TRUE, v);
            assertNull(cause);

            assertFalse(stage.executor().inEventLoop());

            return Boolean.FALSE;
        }), false);
    }

    @Test
    public void testHandleAsyncWithExecutor() {
        EventExecutor asyncExecutor = asyncExecutor();

        testHandle0(newSucceededFuture(), stage -> stage.handleAsync((v, cause) -> {
            assertSame(Boolean.TRUE, v);
            assertNull(cause);

            assertFalse(stage.executor().inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());

            return Boolean.FALSE;
        }, asyncExecutor), false);
    }

    @Test
    public void testHandleThrowable() {
        testHandle0(newFailedFuture(), stage -> stage.handle((v, cause) -> {
            assertSame(EXCEPTION, cause);
            assertNull(v);

            assertTrue(stage.future().executor().inEventLoop());

            return Boolean.FALSE;
        }), false);
    }

    @Test
    public void testHandleAsyncThrowable() {
        testHandle0(newFailedFuture(), stage -> stage.handleAsync((v, cause) -> {
            assertSame(EXCEPTION, cause);
            assertNull(v);

            assertFalse(stage.future().executor().inEventLoop());

            return Boolean.FALSE;
        }), false);
    }

    @Test
    public void testHandleAsyncWithExecutorThrowable() {
        EventExecutor asyncExecutor = asyncExecutor();
        testHandle0(newFailedFuture(), stage -> stage.handleAsync((v, cause) -> {
            assertSame(EXCEPTION, cause);
            assertNull(v);

            assertFalse(stage.future().executor().inEventLoop());
            assertTrue(asyncExecutor.inEventLoop());

            return Boolean.FALSE;
        }, asyncExecutor), false);
    }

    @Test
    public void testHandleFunctionThrows() {
        testHandle0(newSucceededFuture(), stage -> stage.handle((v, cause) -> {
            throw EXCEPTION;
        }), true);
    }

    @Test
    public void testHandleAsyncFunctionThrows() {
        testHandle0(newSucceededFuture(), stage -> stage.handleAsync((v, cause) -> {
            throw EXCEPTION;
        }), true);
    }

    @Test
    public void testHandleAsyncWithExecutorFunctionThrows() {
        EventExecutor asyncExecutor = asyncExecutor();
        testHandle0(newSucceededFuture(), stage -> stage.handleAsync((v, cause) -> {
            throw EXCEPTION;
        }, asyncExecutor), true);
    }
}
