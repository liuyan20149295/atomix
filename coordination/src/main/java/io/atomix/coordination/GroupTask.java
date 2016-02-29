/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.coordination;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupTask<T> implements CatalystSerializable {
  private long id;
  private String member;
  private T value;
  private CompletableFuture<Void> future;

  public GroupTask() {
  }

  public GroupTask(long id, String member, T value) {
    this.id = id;
    this.member = member;
    this.value = value;
  }

  GroupTask<T> setFuture(CompletableFuture<Void> future) {
    this.future = future;
    return this;
  }

  long id() {
    return id;
  }

  String member() {
    return member;
  }

  public T value() {
    return value;
  }

  public void ack() {
    future.complete(null);
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeString(member);
    serializer.writeObject(value, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    member = buffer.readString();
    value = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[member=%s]", getClass().getSimpleName(), member);
  }

}
