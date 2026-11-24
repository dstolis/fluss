/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.api.producer;

import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.kafka.KafkaRequest;
import org.apache.fluss.kafka.KafkaRequestContext;
import org.apache.fluss.kafka.TestingChannelHandlerContext;
import org.apache.fluss.kafka.backend.produce.KafkaProduceBackend;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult;
import org.apache.fluss.kafka.security.KafkaSaslConnection;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link InitProducerIdHandler}. */
class InitProducerIdHandlerTest {

    @Test
    void testNewProducerGetsAnIdAtEpochZero() {
        InitProducerIdResponse response =
                handle(request(null), () -> CompletableFuture.completedFuture(42L));

        assertThat(response.error()).isEqualTo(Errors.NONE);
        assertThat(response.data().producerId()).isEqualTo(42L);
        assertThat(response.data().producerEpoch()).isZero();
    }

    @Test
    void testIdempotenceNotEnabledIsReportedAsMessageFormatError() {
        InitProducerIdResponse response =
                handle(
                        request(null),
                        () ->
                                FutureUtils.completedExceptionally(
                                        new UnsupportedForMessageFormatException("not enabled")));

        assertThat(response.error()).isEqualTo(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT);
        assertThat(response.data().producerId()).isEqualTo(RecordBatch.NO_PRODUCER_ID);
        assertThat(response.data().producerEpoch()).isEqualTo(RecordBatch.NO_PRODUCER_EPOCH);
    }

    @Test
    void testBackendWithoutIdempotenceSupport() {
        KafkaProduceBackend noIdempotence =
                command -> {
                    throw new AssertionError("Produce must not be called");
                };
        InitProducerIdResponse response =
                (InitProducerIdResponse)
                        new InitProducerIdHandler(noIdempotence)
                                .handle(context(), request(null))
                                .join();

        assertThat(response.error()).isEqualTo(Errors.UNSUPPORTED_VERSION);
    }

    @Test
    void testAuthorizationFailureIsReportedAtClusterLevel() {
        assertThat(
                        handle(
                                        request(null),
                                        () ->
                                                FutureUtils.completedExceptionally(
                                                        new AuthorizationException("denied")))
                                .error())
                .isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED);
        // a failure thrown before the future exists is mapped the same way
        assertThat(
                        handle(
                                        request(null),
                                        () -> {
                                            throw new AuthorizationException("denied");
                                        })
                                .error())
                .isEqualTo(Errors.CLUSTER_AUTHORIZATION_FAILED);
    }

    @Test
    void testTransactionalProducerIsRejected() {
        InitProducerIdResponse response =
                handle(
                        request("transaction"),
                        () -> {
                            throw new AssertionError("No producer ID must be allocated");
                        });

        assertThat(response.error()).isEqualTo(Errors.INVALID_REQUEST);
    }

    private static InitProducerIdResponse handle(
            InitProducerIdRequest request, Supplier<CompletableFuture<Long>> producerIds) {
        return (InitProducerIdResponse)
                new InitProducerIdHandler(new TestingBackend(producerIds))
                        .handle(context(), request)
                        .join();
    }

    private static InitProducerIdRequest request(@Nullable String transactionalId) {
        return new InitProducerIdRequest.Builder(
                        new InitProducerIdRequestData()
                                .setTransactionalId(transactionalId)
                                .setTransactionTimeoutMs(Integer.MAX_VALUE))
                .build((short) 4);
    }

    private static KafkaRequestContext context() {
        ByteBuf buffer = Unpooled.buffer(1);
        KafkaRequest request =
                new KafkaRequest(
                        ApiKeys.INIT_PRODUCER_ID,
                        (short) 4,
                        new RequestHeader(ApiKeys.INIT_PRODUCER_ID, (short) 4, "client", 1),
                        request(null),
                        "KAFKA",
                        KafkaSaslConnection.plaintext(),
                        buffer,
                        new TestingChannelHandlerContext(),
                        new CompletableFuture<>()) {};
        buffer.release();
        request.releaseBuffer();
        return KafkaRequestContext.fromRequest(request);
    }

    /** A backend that hands out producer IDs from the given supplier and accepts no records. */
    private static final class TestingBackend implements KafkaProduceBackend {
        private final Supplier<CompletableFuture<Long>> producerIds;

        private TestingBackend(Supplier<CompletableFuture<Long>> producerIds) {
            this.producerIds = producerIds;
        }

        @Override
        public CompletableFuture<KafkaProduceResult> write(KafkaProduceCommand command) {
            throw new AssertionError("Produce must not be called");
        }

        @Override
        public CompletableFuture<Long> initProducerId(
                String listenerName,
                @Nullable InetAddress clientAddress,
                FlussPrincipal principal) {
            return producerIds.get();
        }
    }
}
