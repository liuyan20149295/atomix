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
package io.atomix.primitive.partition.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.atomix.primitive.partition.Member;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.Session;
import io.atomix.storage.buffer.BufferInput;
import io.atomix.storage.buffer.BufferOutput;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.atomix.primitive.partition.impl.PrimaryElectorEvents.CHANGE;

/**
 * State machine for primary elector.
 */
public class PrimaryElectorService extends AbstractPrimitiveService {

  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(PrimaryElectorOperations.NAMESPACE)
      .register(PrimaryElectorEvents.NAMESPACE)
      .register(ElectionState.class)
      .register(Registration.class)
      .register(new LinkedHashMap<>().keySet().getClass())
      .build());

  private Map<PartitionId, AtomicLong> termCounters = new HashMap<>();
  private Map<PartitionId, ElectionState> elections = new HashMap<>();
  private Map<Long, Session> listeners = new LinkedHashMap<>();

  @Override
  public void backup(BufferOutput<?> writer) {
    writer.writeObject(Sets.newHashSet(listeners.keySet()), SERIALIZER::encode);
    writer.writeObject(termCounters, SERIALIZER::encode);
    writer.writeObject(elections, SERIALIZER::encode);
    getLogger().debug("Took state machine snapshot");
  }

  @Override
  public void restore(BufferInput<?> reader) {
    listeners = new LinkedHashMap<>();
    for (Long sessionId : reader.<Set<Long>>readObject(SERIALIZER::decode)) {
      listeners.put(sessionId, getSessions().getSession(sessionId));
    }
    termCounters = reader.readObject(SERIALIZER::decode);
    elections = reader.readObject(SERIALIZER::decode);
    elections.values().forEach(e -> e.elections = elections);
    getLogger().debug("Reinstated state machine from snapshot");
  }

  @Override
  protected void configure(ServiceExecutor executor) {
    // Notification
    executor.register(PrimaryElectorOperations.ADD_LISTENER, this::listen);
    executor.register(PrimaryElectorOperations.REMOVE_LISTENER, this::unlisten);
    // Commands
    executor.register(PrimaryElectorOperations.ENTER, SERIALIZER::decode, this::enter, SERIALIZER::encode);
    executor.register(PrimaryElectorOperations.GET_TERM, SERIALIZER::decode, this::getTerm, SERIALIZER::encode);
  }

  private void notifyTermChange(PartitionId partitionId, PrimaryTerm term) {
    listeners.values().forEach(session -> session.publish(CHANGE, SERIALIZER::encode, new PrimaryElectionEvent(PrimaryElectionEvent.Type.CHANGED, partitionId, term)));
  }

  /**
   * Periodically rebalances leaders.
   */
  private void rebalance() {
    boolean rebalanced = false;
    for (ElectionState election : elections.values()) {
      int leaderCount = election.countLeaders(election.primary);
      int minCandidateCount = 0;
      for (Registration candidate : election.registrations) {
        if (minCandidateCount == 0) {
          minCandidateCount = election.countLeaders(candidate);
        } else {
          minCandidateCount = Math.min(minCandidateCount, election.countLeaders(candidate));
        }
      }

      if (minCandidateCount < leaderCount) {
        for (Registration candidate : election.registrations) {
          if (election.countLeaders(candidate) < leaderCount) {
            PrimaryTerm oldTerm = election.term();
            elections.put(election.partitionId, election.transferLeadership(candidate.member()));
            PrimaryTerm newTerm = term(election.partitionId);
            if (!Objects.equals(oldTerm, newTerm)) {
              notifyTermChange(election.partitionId, newTerm);
              rebalanced = true;
            }
          }
        }
      }
    }

    if (rebalanced) {
      getScheduler().schedule(Duration.ofSeconds(15), this::rebalance);
    }
  }

  /**
   * Applies listen commits.
   *
   * @param commit listen commit
   */
  public void listen(Commit<Void> commit) {
    listeners.put(commit.session().sessionId().id(), commit.session());
  }

  /**
   * Applies unlisten commits.
   *
   * @param commit unlisten commit
   */
  public void unlisten(Commit<Void> commit) {
    listeners.remove(commit.session().sessionId().id());
  }

  /**
   * Applies an {@link PrimaryElectorOperations.Enter} commit.
   *
   * @param commit commit entry
   * @return topic leader. If no previous leader existed this is the node that just entered the race.
   */
  public PrimaryTerm enter(Commit<? extends PrimaryElectorOperations.Enter> commit) {
    try {
      PartitionId partitionId = commit.value().partitionId();
      PrimaryTerm oldTerm = term(partitionId);
      Registration registration = new Registration(
          commit.value().member(),
          commit.session().sessionId().id());
      PrimaryTerm newTerm = elections.compute(partitionId, (k, v) -> {
        if (v == null) {
          return new ElectionState(partitionId, registration, elections);
        } else {
          if (!v.isDuplicate(registration)) {
            return new ElectionState(v).addRegistration(registration);
          } else {
            return v;
          }
        }
      }).term();

      if (!Objects.equals(oldTerm, newTerm)) {
        notifyTermChange(partitionId, newTerm);
        getScheduler().schedule(Duration.ofSeconds(15), this::rebalance);
      }
      return newTerm;
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link PrimaryElectorOperations.GetTerm} commit.
   *
   * @param commit GetLeadership commit
   * @return leader
   */
  public PrimaryTerm getTerm(Commit<? extends PrimaryElectorOperations.GetTerm> commit) {
    PartitionId partitionId = commit.value().partitionId();
    try {
      return term(partitionId);
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  private PrimaryTerm term(PartitionId partitionId) {
    ElectionState electionState = elections.get(partitionId);
    return electionState != null ? electionState.term() : null;
  }

  private void onSessionEnd(Session session) {
    listeners.remove(session.sessionId().id());
    Set<PartitionId> partitions = elections.keySet();
    partitions.forEach(partitionId -> {
      PrimaryTerm oldTerm = term(partitionId);
      elections.compute(partitionId, (k, v) -> v.cleanup(session));
      PrimaryTerm newTerm = term(partitionId);
      if (!Objects.equals(oldTerm, newTerm)) {
        notifyTermChange(partitionId, newTerm);
        getScheduler().schedule(Duration.ofSeconds(15), this::rebalance);
      }
    });
  }

  private static class Registration {
    private final Member member;
    private final long sessionId;

    public Registration(Member member, long sessionId) {
      this.member = member;
      this.sessionId = sessionId;
    }

    public Member member() {
      return member;
    }

    public long sessionId() {
      return sessionId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("member", member)
          .add("session", sessionId)
          .toString();
    }
  }

  private static class ElectionState {
    private final PartitionId partitionId;
    private final Registration primary;
    private final long term;
    private final long termStartTime;
    private final List<Registration> registrations;
    private transient Map<PartitionId, ElectionState> elections;

    public ElectionState(
        PartitionId partitionId,
        Registration registration,
        Map<PartitionId, ElectionState> elections) {
      registrations = Arrays.asList(registration);
      termStartTime = System.currentTimeMillis();
      primary = registration;
      this.partitionId = partitionId;
      this.term = 1;
      this.elections = elections;
    }

    public ElectionState(ElectionState other) {
      partitionId = other.partitionId;
      registrations = Lists.newArrayList(other.registrations);
      primary = other.primary;
      term = other.term;
      termStartTime = other.termStartTime;
      elections = other.elections;
    }

    public ElectionState(
        PartitionId partitionId,
        List<Registration> registrations,
        Registration primary,
        long term,
        long termStartTime,
        Map<PartitionId, ElectionState> elections) {
      this.partitionId = partitionId;
      this.registrations = Lists.newArrayList(registrations);
      this.primary = primary;
      this.term = term;
      this.termStartTime = termStartTime;
      this.elections = elections;
    }

    public ElectionState cleanup(Session session) {
      Optional<Registration> registration =
          registrations.stream().filter(r -> r.sessionId() == session.sessionId().id()).findFirst();
      if (registration.isPresent()) {
        List<Registration> updatedRegistrations =
            registrations.stream()
                .filter(r -> r.sessionId() != session.sessionId().id())
                .collect(Collectors.toList());
        if (primary.sessionId() == session.sessionId().id()) {
          if (!updatedRegistrations.isEmpty()) {
            return new ElectionState(
                partitionId,
                updatedRegistrations,
                updatedRegistrations.get(0),
                term + 1,
                System.currentTimeMillis(),
                elections);
          } else {
            return new ElectionState(
                partitionId,
                updatedRegistrations,
                null,
                term,
                termStartTime,
                elections);
          }
        } else {
          return new ElectionState(
              partitionId,
              updatedRegistrations,
              primary,
              term,
              termStartTime,
              elections);
        }
      } else {
        return this;
      }
    }

    public boolean isDuplicate(Registration registration) {
      return registrations.stream()
          .anyMatch(r -> r.sessionId() == registration.sessionId());
    }

    public PrimaryTerm term() {
      return new PrimaryTerm(term, primary(), candidates());
    }

    public Member primary() {
      if (primary == null) {
        return null;
      } else {
        return primary.member();
      }
    }

    public List<Member> candidates() {
      return registrations.stream().map(registration -> registration.member()).collect(Collectors.toList());
    }

    ElectionState addRegistration(Registration registration) {
      if (!registrations.stream().anyMatch(r -> r.sessionId() == registration.sessionId())) {
        List<Registration> updatedRegistrations = new LinkedList<>(registrations);

        boolean added = false;
        int registrationCount = countLeaders(registration);
        for (int i = 0; i < registrations.size(); i++) {
          if (countLeaders(registrations.get(i)) > registrationCount) {
            updatedRegistrations.set(i, registration);
            added = true;
            break;
          }
        }

        if (!added) {
          updatedRegistrations.add(registration);
        }

        Registration firstRegistration = updatedRegistrations.get(0);
        Registration leader = this.primary;
        long term = this.term;
        long termStartTime = this.termStartTime;
        if (leader == null || !leader.equals(firstRegistration)) {
          leader = firstRegistration;
          term = this.term + 1;
          termStartTime = System.currentTimeMillis();
        }
        return new ElectionState(
            partitionId,
            updatedRegistrations,
            leader,
            term,
            termStartTime,
            elections);
      }
      return this;
    }

    int countLeaders(Registration registration) {
      return (int) elections.entrySet().stream()
          .filter(entry -> !entry.getKey().equals(partitionId))
          .filter(entry -> entry.getValue().primary != null)
          .filter(entry -> {
            // Get the topic leader's identifier and a list of session identifiers.
            // Then return true if the leader's identifier matches any of the session's candidates.
            Member leaderId = entry.getValue().primary();
            List<Member> sessionCandidates = entry.getValue().registrations.stream()
                .filter(r -> r.sessionId == registration.sessionId)
                .map(r -> r.member())
                .collect(Collectors.toList());
            return sessionCandidates.stream()
                .anyMatch(candidate -> Objects.equals(candidate, leaderId));
          })
          .count();
    }

    ElectionState transferLeadership(Member member) {
      Registration newLeader = registrations.stream()
          .filter(r -> Objects.equals(r.member(), member))
          .findFirst()
          .orElse(null);
      if (newLeader != null) {
        return new ElectionState(
            partitionId,
            registrations,
            newLeader,
            term + 1,
            System.currentTimeMillis(),
            elections);
      } else {
        return this;
      }
    }
  }

  @Override
  public void onExpire(Session session) {
    onSessionEnd(session);
  }

  @Override
  public void onClose(Session session) {
    onSessionEnd(session);
  }
}