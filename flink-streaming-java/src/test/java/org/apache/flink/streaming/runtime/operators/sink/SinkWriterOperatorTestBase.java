/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/** Base class for Tests for subclasses of {@link AbstractSinkWriterOperator}. */
public abstract class SinkWriterOperatorTestBase extends TestLogger {

    @Rule public SharedObjects sharedObjects = SharedObjects.create();

    protected abstract AbstractSinkWriterOperatorFactory<Integer, String> createWriterOperator(
            TestSink sink);

    @Test
    public void nonBufferingWriterEmitsWithoutFlush() throws Exception {
        final long initialTime = 0;

        final OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                createTestHarness(
                        TestSink.newBuilder()
                                .setWriter(new TestSink.DefaultSinkWriter())
                                .withWriterState()
                                .build());
        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.prepareSnapshotPreBarrier(1L);
        testHarness.snapshot(1L, 1L);

        assertThat(
                testHarness.getOutput(),
                contains(
                        new Watermark(initialTime),
                        new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime).toString()),
                        new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime).toString())));
    }

    @Test
    public void nonBufferingWriterEmitsOnFlush() throws Exception {
        final long initialTime = 0;

        final OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                createTestHarness(
                        TestSink.newBuilder()
                                .setWriter(new TestSink.DefaultSinkWriter())
                                .withWriterState()
                                .build());
        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.endInput();

        assertThat(
                testHarness.getOutput(),
                contains(
                        new Watermark(initialTime),
                        new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime).toString()),
                        new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime).toString())));
    }

    @Test
    public void bufferingWriterDoesNotEmitWithoutFlush() throws Exception {
        final long initialTime = 0;

        final OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                createTestHarness(
                        TestSink.newBuilder()
                                .setWriter(new BufferingSinkWriter())
                                .withWriterState()
                                .build());
        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.prepareSnapshotPreBarrier(1L);
        testHarness.snapshot(1L, 1L);

        assertThat(testHarness.getOutput(), contains(new Watermark(initialTime)));
    }

    @Test
    public void bufferingWriterEmitsOnFlush() throws Exception {
        final long initialTime = 0;

        final OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                createTestHarness(
                        TestSink.newBuilder()
                                .setWriter(new BufferingSinkWriter())
                                .withWriterState()
                                .build());
        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.endInput();

        assertThat(
                testHarness.getOutput(),
                contains(
                        new Watermark(initialTime),
                        new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime).toString()),
                        new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime).toString())));
    }

    @Test
    public void timeBasedBufferingSinkWriter() throws Exception {
        final long initialTime = 0;

        final OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                createTestHarness(
                        TestSink.newBuilder()
                                .setWriter(new TimeBasedBufferingSinkWriter())
                                .withWriterState()
                                .build());
        testHarness.open();

        testHarness.setProcessingTime(0L);

        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.prepareSnapshotPreBarrier(1L);

        assertThat(testHarness.getOutput().size(), equalTo(0));

        testHarness.getProcessingTimeService().setCurrentTime(2001);

        testHarness.prepareSnapshotPreBarrier(2L);
        testHarness.endInput();

        assertThat(
                testHarness.getOutput(),
                contains(
                        new StreamRecord<>(
                                Tuple3.of(1, initialTime + 1, Long.MIN_VALUE).toString()),
                        new StreamRecord<>(
                                Tuple3.of(2, initialTime + 2, Long.MIN_VALUE).toString())));
    }

    @Test
    public void watermarkPropagatedToSinkWriter() throws Exception {
        final long initialTime = 0;

        final TestSink.DefaultSinkWriter writer = new TestSink.DefaultSinkWriter();
        final OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                createTestHarness(
                        TestSink.newBuilder().setWriter(writer).withWriterState().build());
        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processWatermark(initialTime + 1);

        assertThat(
                testHarness.getOutput(),
                contains(new Watermark(initialTime), new Watermark(initialTime + 1)));
        assertThat(
                writer.watermarks,
                contains(
                        new org.apache.flink.api.common.eventtime.Watermark(initialTime),
                        new org.apache.flink.api.common.eventtime.Watermark(initialTime + 1)));
    }

    @Test
    public void testInitContext() throws Exception {
        SharedReference<AtomicBoolean> mailExecuted = sharedObjects.add(new AtomicBoolean(false));
        TestSink.DefaultSinkWriter writer = new InitVerifyingWriter(mailExecuted);

        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);
        TestSink sink = TestSink.newBuilder().setWriter(writer).withWriterState().build();
        try (StreamTaskMailboxTestHarness<Integer> harness =
                builder.setupOutputForSingletonOperatorChain(createWriterOperator(sink)).build()) {
            harness.processAll();
        }

        // verify that async processing worked as expected
        assertThat(mailExecuted.get().get(), equalTo(true));
    }

    /**
     * A {@link SinkWriter} that only returns committables from {@link #prepareCommit(boolean)} when
     * {@code flush} is {@code true}.
     */
    static class BufferingSinkWriter extends TestSink.DefaultSinkWriter {
        @Override
        public List<String> prepareCommit(boolean flush) {
            if (!flush) {
                return Collections.emptyList();
            }
            List<String> result = elements;
            elements = new ArrayList<>();
            return result;
        }
    }

    /**
     * A {@link SinkWriter} that buffers the committables and send the cached committables per
     * second.
     */
    static class TimeBasedBufferingSinkWriter extends TestSink.DefaultSinkWriter
            implements Sink.ProcessingTimeService.ProcessingTimeCallback {

        private final List<String> cachedCommittables = new ArrayList<>();

        @Override
        public void write(Integer element, Context context) {
            cachedCommittables.add(
                    Tuple3.of(element, context.timestamp(), context.currentWatermark()).toString());
        }

        void setProcessingTimerService(Sink.ProcessingTimeService processingTimerService) {
            super.setProcessingTimerService(processingTimerService);
            this.processingTimerService.registerProcessingTimer(1000, this);
        }

        @Override
        public void onProcessingTime(long time) throws IOException {
            elements.addAll(cachedCommittables);
            cachedCommittables.clear();
            this.processingTimerService.registerProcessingTimer(time + 1000, this);
        }
    }

    protected OneInputStreamOperatorTestHarness<Integer, String> createTestHarness(TestSink sink)
            throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                createWriterOperator(sink), IntSerializer.INSTANCE);
    }

    private static class InitVerifyingWriter extends TestSink.DefaultSinkWriter {
        private final SharedReference<AtomicBoolean> mailExecuted;

        public InitVerifyingWriter(SharedReference<AtomicBoolean> mailExecuted) {
            this.mailExecuted = mailExecuted;
        }

        @Override
        public void init(Sink.InitContext context) {
            super.init(context);
            assertThat(context.getUserCodeClassLoader(), notNullValue());
            assertThat(context.getProcessingTimeService(), notNullValue());
            assertThat(context.getMailboxExecutor(), notNullValue());
            assertThat(context.metricGroup(), notNullValue());
            assertThat(context.getSubtaskId(), equalTo(0));
            assertThat(context.getNumberOfParallelSubtasks(), equalTo(1));

            context.getMailboxExecutor().execute(() -> mailExecuted.get().set(true), "mail");
        }
    }
}
