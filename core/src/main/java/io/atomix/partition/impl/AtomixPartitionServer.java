/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.partition.impl;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.protocol.messaging.RaftServerCommunicator;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.impl.StorageNamespaces;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.Managed;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * {@link AtomixPartition} server.
 */
public class AtomixPartitionServer implements Managed<AtomixPartitionServer> {

  private final Logger log = getLogger(getClass());

  private static final int MAX_SEGMENT_SIZE = 1024 * 1024 * 64;
  private static final long ELECTION_TIMEOUT_MILLIS = 2500;
  private static final long HEARTBEAT_INTERVAL_MILLIS = 250;

  private final MemberId localMemberId;
  private final AtomixPartition partition;
  private final ClusterCommunicationService clusterCommunicator;
  private RaftServer server;

  public AtomixPartitionServer(
      AtomixPartition partition,
      MemberId localMemberId,
      ClusterCommunicationService clusterCommunicator) {
    this.partition = partition;
    this.localMemberId = localMemberId;
    this.clusterCommunicator = clusterCommunicator;
  }

  @Override
  public CompletableFuture<AtomixPartitionServer> open() {
    log.info("Starting server for partition {}", partition.getId());
    CompletableFuture<RaftServer> serverOpenFuture;
    if (partition.getMemberIds().contains(localMemberId)) {
      if (server != null && server.isRunning()) {
        return CompletableFuture.completedFuture(null);
      }
      synchronized (this) {
        server = buildServer();
      }
      serverOpenFuture = server.bootstrap(partition.getMemberIds());
    } else {
      serverOpenFuture = CompletableFuture.completedFuture(null);
    }
    return serverOpenFuture.whenComplete((r, e) -> {
      if (e == null) {
        log.info("Successfully started server for partition {}", partition.getId());
      } else {
        log.info("Failed to start server for partition {}", partition.getId(), e);
      }
    }).thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return server.shutdown();
  }

  /**
   * Closes the server and exits the partition.
   *
   * @return future that is completed when the operation is complete
   */
  public CompletableFuture<Void> closeAndExit() {
    return server.leave();
  }

  /**
   * Deletes the server.
   */
  public void delete() {
    try {
      Files.walkFileTree(partition.getDataFolder().toPath(), new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (IOException e) {
      log.error("Failed to delete partition: {}", e);
    }
  }

  private RaftServer buildServer() {
    RaftServer.Builder builder = RaftServer.newBuilder(localMemberId)
        .withName(String.format("partition-%s", partition.getId()))
        .withProtocol(new RaftServerCommunicator(
            String.format("partition-%d", partition.getId().id()),
            Serializer.using(StorageNamespaces.RAFT_PROTOCOL),
            clusterCommunicator))
        .withElectionTimeout(Duration.ofMillis(ELECTION_TIMEOUT_MILLIS))
        .withHeartbeatInterval(Duration.ofMillis(HEARTBEAT_INTERVAL_MILLIS))
        .withStorage(RaftStorage.newBuilder()
            .withPrefix(String.format("partition-%s", partition.getId()))
            .withStorageLevel(StorageLevel.MAPPED)
            .withSerializer(Serializer.using(StorageNamespaces.RAFT_STORAGE))
            .withDirectory(partition.getDataFolder())
            .withMaxSegmentSize(MAX_SEGMENT_SIZE)
            .build());
    AtomixPartition.RAFT_SERVICES.forEach(builder::addService);
    return builder.build();
  }

  public CompletableFuture<Void> join(Collection<MemberId> otherMembers) {
    log.info("Joining partition {} ({})", partition.getId(), partition.getName());
    server = buildServer();
    return server.join(otherMembers).whenComplete((r, e) -> {
      if (e == null) {
        log.info("Successfully joined partition {} ({})", partition.getId(), partition.getName());
      } else {
        log.info("Failed to join partition {} ({})", partition.getId(), partition.getName(), e);
      }
    }).thenApply(v -> null);
  }

  @Override
  public boolean isOpen() {
    return server.isRunning();
  }

  @Override
  public boolean isClosed() {
    return !isOpen();
  }
}
