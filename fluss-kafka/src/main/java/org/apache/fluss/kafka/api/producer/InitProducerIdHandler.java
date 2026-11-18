/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.kafka.KafkaRequestContext;
import org.apache.fluss.kafka.backend.produce.KafkaProduceBackend;
import org.apache.fluss.kafka.dispatcher.KafkaApiHandler;
import org.apache.fluss.kafka.dispatcher.KafkaApiSpec;
import org.apache.fluss.kafka.error.KafkaErrorMapper;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Implements Kafka InitProducerId versions 0 through 5 for idempotent producers without
 * transactions.
 *
 * <p>The producer ID is a Fluss writer ID, taken from the same counter as the IDs of Fluss writers.
 * A new producer starts at epoch 0. An idempotent producer bumps its epoch itself and sends it with
 * its batches, and asks for a new ID when the epoch runs out, so every request gets a new ID.
 */
@Internal
public final class InitProducerIdHandler implements KafkaApiHandler<InitProducerIdRequest> {

    private static final KafkaApiSpec API_SPEC =
            new KafkaApiSpec(ApiKeys.INIT_PRODUCER_ID, (short) 0, (short) 5, true);

    private final KafkaProduceBackend backend;

    /** Creates an InitProducerId handler. */
    public InitProducerIdHandler(KafkaProduceBackend backend) {
        this.backend = checkNotNull(backend);
    }

    @Override
    public KafkaApiSpec apiSpec() {
        return API_SPEC;
    }

    @Override
    public CompletableFuture<? extends AbstractResponse> handle(
            KafkaRequestContext context, InitProducerIdRequest request) {
        if (request.data().transactionalId() != null) {
            return CompletableFuture.completedFuture(failed(Errors.INVALID_REQUEST));
        }
        CompletableFuture<Long> producerId;
        try {
            producerId =
                    checkNotNull(
                            backend.initProducerId(
                                    context.listenerName(),
                                    clientAddress(context),
                                    context.principal()));
        } catch (RuntimeException failure) {
            return CompletableFuture.completedFuture(failed(toError(failure)));
        }
        return producerId.handle(
                (id, failure) ->
                        failure != null
                                ? failed(toError(failure))
                                : new InitProducerIdResponse(
                                        new InitProducerIdResponseData()
                                                .setErrorCode(Errors.NONE.code())
                                                .setProducerId(id)
                                                .setProducerEpoch((short) 0)));
    }

    private static Errors toError(Throwable failure) {
        Throwable cause = KafkaErrorMapper.unwrap(failure);
        if (cause instanceof ApiException) {
            return Errors.forException(cause);
        }
        if (cause instanceof AuthorizationException) {
            // Kafka reports a producer that may not write idempotently at the cluster level
            return Errors.CLUSTER_AUTHORIZATION_FAILED;
        }
        return KafkaErrorMapper.toKafkaError(
                org.apache.fluss.rpc.protocol.Errors.forException(cause));
    }

    private static InitProducerIdResponse failed(Errors error) {
        return new InitProducerIdResponse(
                new InitProducerIdResponseData()
                        .setErrorCode(error.code())
                        .setProducerId(RecordBatch.NO_PRODUCER_ID)
                        .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH));
    }

    private static InetAddress clientAddress(KafkaRequestContext context) {
        return context.remoteAddress() instanceof InetSocketAddress
                ? ((InetSocketAddress) context.remoteAddress()).getAddress()
                : null;
    }
}
