/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.cdc;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * A Kafka {@link Partitioner} that routes messages to the same partition as the Cassandra
 * partition token, preserving ordering guarantees consistent with Cassandra's Murmur3 partitioner.
 *
 * <p>Because Kafka's {@link Partitioner} interface does not expose record headers, the Cassandra
 * token is conveyed via a {@link ThreadLocal}. The sender sets {@link #TOKEN_HOLDER} before
 * calling {@code producer.send()} and the ThreadLocal is cleared in the send callback.
 *
 * <p>The same token-to-partition mapping as {@link Murmur3MessageRouter} is used:
 * {@code partition = ((short)(token >>> 48) + Short.MAX_VALUE + 1) % numPartitions}
 */
public class KafkaMurmur3Partitioner implements Partitioner {

    /**
     * Holds the Cassandra partition token for the record currently being partitioned.
     * Must be set by the producer thread before calling {@code send()} and cleared in the callback.
     */
    public static final ThreadLocal<Object> TOKEN_HOLDER = new ThreadLocal<>();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(topic);
        if (numPartitions <= 0) {
            return 0;
        }
        Object tokenObj = TOKEN_HOLDER.get();
        if (tokenObj == null) {
            // Fall back to default hash of keyBytes if token not set
            return (keyBytes == null ? 0 : Math.abs(java.util.Arrays.hashCode(keyBytes)) % numPartitions);
        }
        long token;
        if (tokenObj instanceof Long) {
            token = (Long) tokenObj;
        } else {
            try {
                token = Long.parseLong(tokenObj.toString());
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return ((short) (token >>> 48) + Short.MAX_VALUE + 1) % numPartitions;
    }

    @Override
    public void close() {
        // nothing to close
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // no configuration needed
    }
}
