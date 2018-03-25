/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitive.partition.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.partition.ManagedPrimaryElection;
import io.atomix.primitive.partition.ManagedPrimaryElectionService;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.AtomixRuntimeException;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Leader elector based primary election service.
 */
public class DefaultPrimaryElectionService implements ManagedPrimaryElectionService {
  private static final String PRIMITIVE_NAME = "atomix-primary-elector";

  private final PartitionGroup partitions;
  private final PrimitiveProtocol protocol;
  private final Set<PrimaryElectionEventListener> listeners = Sets.newIdentityHashSet();
  private final PrimaryElectionEventListener eventListener = event -> listeners.forEach(l -> l.onEvent(event));
  private final Map<PartitionId, ManagedPrimaryElection> elections = Maps.newConcurrentMap();
  private final AtomicBoolean started = new AtomicBoolean();

  public DefaultPrimaryElectionService(PartitionGroup partitionGroup, PrimitiveProtocol protocol) {
    this.partitions = checkNotNull(partitionGroup);
    this.protocol = checkNotNull(protocol);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimaryElection getElectionFor(PartitionId partitionId) {
    ManagedPrimaryElection election = elections.get(partitionId);
    if (election == null) {
      synchronized (elections) {
        election = elections.get(partitionId);
        if (election == null) {
          try {
            PrimitiveProxy proxy = partitions.getPartitions().iterator().next().getPrimitiveClient()
                .newProxy(PRIMITIVE_NAME, PrimaryElectorType.instance(), protocol)
                .connect()
                .get(1, TimeUnit.MINUTES);
            election = new DefaultPrimaryElection(partitionId, proxy);
            election.addListener(eventListener);
            elections.put(partitionId, election);
          } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new AtomixRuntimeException(e);
          }
        }
      }
    }
    return election;
  }

  @Override
  public void addListener(PrimaryElectionEventListener listener) {
    listeners.add(checkNotNull(listener));
  }

  @Override
  public void removeListener(PrimaryElectionEventListener listener) {
    listeners.remove(checkNotNull(listener));
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<PrimaryElectionService> start() {
    started.set(true);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }
}
