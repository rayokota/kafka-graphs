/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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
package io.kgraph.pregel;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PregelConsumer implements Consumer<byte[], byte[]> {
    private static final Logger log = LoggerFactory.getLogger(PregelConsumer.class);

    private final Consumer<byte[], byte[]> kafkaConsumer;

    public PregelConsumer(Consumer<byte[], byte[]> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public Set<TopicPartition> assignment() {
        return kafkaConsumer.assignment();
    }

    @Override
    public Set<String> subscription() {
        return kafkaConsumer.subscription();
    }

    @Override
    public void subscribe(Collection<String> topics) {
        kafkaConsumer.subscribe(topics);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        kafkaConsumer.subscribe(topics, callback);
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        kafkaConsumer.assign(partitions);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        kafkaConsumer.subscribe(pattern, callback);
    }

    @Override
    public void subscribe(Pattern pattern) {
        kafkaConsumer.subscribe(pattern);
    }

    @Override
    public void unsubscribe() {
        kafkaConsumer.unsubscribe();
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(long timeout) {
        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(timeout);
        log.trace("poll {}", records.partitions());
        return records;
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(timeout);
        log.trace("poll {}", records.partitions());
        return records;
    }

    @Override
    public void commitSync() {
        kafkaConsumer.commitSync();
    }

    @Override
    public void commitSync(Duration timeout) {
        kafkaConsumer.commitSync(timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        kafkaConsumer.commitSync(offsets);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        kafkaConsumer.commitSync(offsets, timeout);
    }

    @Override
    public void commitAsync() {
        kafkaConsumer.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        kafkaConsumer.commitAsync(callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        kafkaConsumer.commitAsync(offsets, callback);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        kafkaConsumer.seek(partition, offset);
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        kafkaConsumer.seek(partition, offsetAndMetadata);
    }
    
    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        kafkaConsumer.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        kafkaConsumer.seekToEnd(partitions);
    }

    @Override
    public long position(TopicPartition partition) {
        return kafkaConsumer.position(partition);
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return kafkaConsumer.position(partition, timeout);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return kafkaConsumer.committed(partition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return kafkaConsumer.committed(partition, timeout);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return kafkaConsumer.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return kafkaConsumer.partitionsFor(topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return kafkaConsumer.partitionsFor(topic, timeout);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return kafkaConsumer.listTopics();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return kafkaConsumer.listTopics(timeout);
    }

    @Override
    public Set<TopicPartition> paused() {
        return kafkaConsumer.paused();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        kafkaConsumer.pause(partitions);
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        kafkaConsumer.resume(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return kafkaConsumer.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return kafkaConsumer.offsetsForTimes(timestampsToSearch, timeout);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return kafkaConsumer.beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return kafkaConsumer.beginningOffsets(partitions, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return kafkaConsumer.endOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return kafkaConsumer.endOffsets(partitions, timeout);
    }

    @Override
    public void close() {
        kafkaConsumer.close();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        kafkaConsumer.close(timeout, unit);
    }

    @Override
    public void close(Duration timeout) {
        kafkaConsumer.close(timeout);
    }

    @Override
    public void wakeup() {
        kafkaConsumer.wakeup();
    }
}
