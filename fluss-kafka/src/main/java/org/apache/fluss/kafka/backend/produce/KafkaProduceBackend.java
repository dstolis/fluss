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

package org.apache.fluss.kafka.backend.produce;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;

/** Narrow backend used by the Kafka Produce API. */
@Internal
public interface KafkaProduceBackend extends AutoCloseable {
    /**
     * Consumes Kafka records synchronously and writes them through the native Fluss write path.
     *
     * <p>All accesses to borrowed command payloads must finish before this method returns or
     * throws. Record keys and values are borrowed read-only views of storage that may belong to the
     * network request; header values are borrowed byte arrays. The caller keeps these bytes alive
     * and unchanged during this call, and consumers must not modify them. Input storage can be
     * released or reused as soon as request handling returns, without waiting for the returned
     * future. Read-only views do not extend the storage's lifetime.
     *
     * <p>Implementations must decode the payloads into independent objects (for example JSON
     * objects or Fluss rows) before returning. Asynchronous work must use only independently owned
     * data and must not retain the command's records, buffers or header arrays. The returned future
     * represents completion of the write, not permission to continue reading the input payloads.
     */
    CompletableFuture<KafkaProduceResult> write(KafkaProduceCommand command);

    /**
     * Allocates the producer ID of a new idempotent producer. It is a Fluss writer ID, so the
     * records of the producer are checked like those of a Fluss writer.
     *
     * <p>A backend that supports idempotent producers fails the future with {@link
     * UnsupportedForMessageFormatException} while the cluster does not allow the log record batch
     * format that stores what idempotence needs. The default implementation is for backends without
     * idempotent producer support and fails it with {@link UnsupportedVersionException}.
     */
    default CompletableFuture<Long> initProducerId(
            String listenerName, @Nullable InetAddress clientAddress, FlussPrincipal principal) {
        return FutureUtils.completedExceptionally(
                new UnsupportedVersionException(
                        "This backend does not support idempotent producers."));
    }

    /** Releases backend resources after request processing stops. */
    @Override
    default void close() throws Exception {}
}
