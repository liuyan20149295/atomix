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

import com.google.common.collect.Sets;
import io.atomix.primitive.partition.ManagedPrimaryElection;
import io.atomix.primitive.partition.Member;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.Serializer;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.primitive.partition.impl.PrimaryElectorEvents.CHANGE;
import static io.atomix.primitive.partition.impl.PrimaryElectorOperations.ADD_LISTENER;
import static io.atomix.primitive.partition.impl.PrimaryElectorOperations.ENTER;
import static io.atomix.primitive.partition.impl.PrimaryElectorOperations.Enter;
import static io.atomix.primitive.partition.impl.PrimaryElectorOperations.GET_TERM;
import static io.atomix.primitive.partition.impl.PrimaryElectorOperations.GetTerm;
import static io.atomix.primitive.partition.impl.PrimaryElectorOperations.REMOVE_LISTENER;

/**
 * Leader elector based primary election.
 */
public class DefaultPrimaryElection implements ManagedPrimaryElection {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(PrimaryElectorOperations.NAMESPACE)
      .register(PrimaryElectorEvents.NAMESPACE)
      .build());

  private final PartitionId partitionId;
  private final PrimitiveProxy proxy;
  private final Set<PrimaryElectionEventListener> listeners = Sets.newCopyOnWriteArraySet();
  private final Consumer<PrimaryElectionEvent> eventListener;
  private final AtomicBoolean started = new AtomicBoolean();

  public DefaultPrimaryElection(PartitionId partitionId, PrimitiveProxy proxy) {
    this.partitionId = checkNotNull(partitionId);
    this.proxy = proxy;
    this.eventListener = event -> {
      if (event.partitionId().equals(partitionId)) {
        listeners.forEach(l -> l.onEvent(event));
      }
    };
  }

  @Override
  public CompletableFuture<PrimaryTerm> enter(Member member) {
    return proxy.invoke(ENTER, SERIALIZER::encode, new Enter(partitionId, member), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<PrimaryTerm> getTerm() {
    return proxy.invoke(GET_TERM, SERIALIZER::encode, new GetTerm(partitionId), SERIALIZER::decode);
  }

  @Override
  public synchronized void addListener(PrimaryElectionEventListener listener) {
    if (listeners.isEmpty()) {
      proxy.invoke(ADD_LISTENER);
    }
    listeners.add(checkNotNull(listener));
  }

  @Override
  public synchronized void removeListener(PrimaryElectionEventListener listener) {
    listeners.remove(checkNotNull(listener));
    if (listeners.isEmpty()) {
      proxy.invoke(REMOVE_LISTENER);
    }
  }

  @Override
  public CompletableFuture<PrimaryElection> start() {
    return proxy.connect()
        .thenAccept(proxy -> {
          proxy.addEventListener(CHANGE, SERIALIZER::decode, eventListener);
          started.set(true);
        })
        .thenApply(v -> this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    return proxy.close().thenRun(() -> started.set(false));
  }

  @Override
  public void close() {
    proxy.close();
  }
}
