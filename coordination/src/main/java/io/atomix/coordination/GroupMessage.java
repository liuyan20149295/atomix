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
 * Group member message.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupMessage<T> implements CatalystSerializable {
  private String member;
  private String topic;
  private T body;
  private CompletableFuture<Object> future;

  public GroupMessage() {
  }

  public GroupMessage(String member, String topic, T body) {
    this.member = member;
    this.topic = topic;
    this.body = body;
  }

  GroupMessage<T> setFuture(CompletableFuture<Object> future) {
    this.future = future;
    return this;
  }

  String member() {
    return member;
  }

  public String topic() {
    return topic;
  }

  public T body() {
    return body;
  }

  public void reply(Object reply) {
    future.complete(reply);
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeString(member);
    buffer.writeString(topic);
    serializer.writeObject(body, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    member = buffer.readString();
    topic = buffer.readString();
    body = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[member=%s, topic=%s]", getClass().getSimpleName(), member, topic);
  }

}
