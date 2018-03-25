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
package io.atomix.primitive.partition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Partition primary term.
 */
public class PrimaryTerm {
  private final long term;
  private final Member primary;
  private final List<Member> candidates;

  public PrimaryTerm(long term, Member primary, List<Member> candidates) {
    this.term = term;
    this.primary = primary;
    this.candidates = candidates;
  }

  /**
   * Returns the primary term number.
   *
   * @return the primary term number
   */
  public long term() {
    return term;
  }

  /**
   * Returns the primary node identifier.
   *
   * @return the primary node identifier
   */
  public Member primary() {
    return primary;
  }

  /**
   * Returns the list of candidate nodes.
   *
   * @return the list of candidate nodes
   */
  public List<Member> candidates() {
    return candidates;
  }

  /**
   * Returns the backup nodes.
   *
   * @return the backup nodes
   */
  public List<Member> backups(int numBackups) {
    if (primary == null) {
      return new ArrayList<>(0);
    }

    List<Member> backups = new ArrayList<>();
    Set<MemberGroupId> groups = new HashSet<>();
    groups.add(primary.groupId());

    int i = 0;
    for (int j = 0; j < numBackups; j++) {
      while (i < candidates.size()) {
        Member member = candidates.get(i++);
        if (groups.add(member.groupId())) {
          backups.add(member);
          break;
        }
      }
    }
    return backups;
  }

  @Override
  public int hashCode() {
    return Objects.hash(term, primary, candidates);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PrimaryTerm) {
      PrimaryTerm term = (PrimaryTerm) object;
      return term.term == this.term
          && Objects.equals(term.primary, primary)
          && Objects.equals(term.candidates, candidates);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("primary", primary)
        .add("backups", candidates)
        .toString();
  }
}
