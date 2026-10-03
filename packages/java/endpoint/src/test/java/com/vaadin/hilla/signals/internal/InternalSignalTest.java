/*
 * Copyright 2000-2025 Vaadin Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.vaadin.hilla.signals.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import com.vaadin.flow.signals.Id;
import com.vaadin.flow.signals.shared.SharedNumberSignal;

public class InternalSignalTest {

    private static final String CLIENT_SIGNAL_ID_1 = "90000000-9000-9000-9000-900000000000";
    private static final String CLIENT_SIGNAL_ID_2 = "80000000-8000-8000-8000-800000000000";
    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(5);

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void when_subscriberIsAttached_serverSet_emitsCommandAndKeepsSubscriber() {
        SharedNumberSignal numberSignal = new SharedNumberSignal(1.0);
        InternalSignal internalSignal = new InternalSignal(numberSignal,
                mapper);

        // publishOn keeps StepVerifier off the signal-tree callback stack.
        StepVerifier
                .create(internalSignal.subscribe(CLIENT_SIGNAL_ID_1)
                        .publishOn(Schedulers.parallel()))
                .consumeNextWith(event -> assertSnapshot(event, 1.0))
                .then(() -> numberSignal.set(4))
                .consumeNextWith(event -> assertServerSet(event, 4.0))
                .then(() -> numberSignal.set(9))
                .consumeNextWith(event -> assertServerSet(event, 9.0))
                .thenCancel().verify(VERIFY_TIMEOUT);
    }

    @Test
    public void when_twoSubscribersAreAttached_serverSet_reachesBoth() {
        SharedNumberSignal numberSignal = new SharedNumberSignal(1.0);
        InternalSignal internalSignal = new InternalSignal(numberSignal,
                mapper);

        Flux<Tuple2<String, JsonNode>> events = Flux
                .merge(internalSignal.subscribe(CLIENT_SIGNAL_ID_1)
                        .map(event -> Tuples.of(CLIENT_SIGNAL_ID_1, event)),
                        internalSignal.subscribe(CLIENT_SIGNAL_ID_2).map(
                                event -> Tuples.of(CLIENT_SIGNAL_ID_2, event)))
                .publishOn(Schedulers.parallel());

        StepVerifier.create(events).recordWith(ArrayList::new)
                .expectNextCount(2).then(() -> numberSignal.set(4))
                .expectNextCount(2).then(() -> numberSignal.set(9))
                .expectNextCount(2).consumeRecordedWith(recorded -> {
                    List<Tuple2<String, JsonNode>> list = new ArrayList<>(
                            recorded);
                    assertEquals(6, list.size());
                    assertSnapshotFor(list.get(0), CLIENT_SIGNAL_ID_1, 1.0);
                    assertSnapshotFor(list.get(1), CLIENT_SIGNAL_ID_2, 1.0);
                    assertServerSetReachedBoth(list.subList(2, 4), 4.0);
                    assertServerSetReachedBoth(list.subList(4, 6), 9.0);
                }).thenCancel().verify(VERIFY_TIMEOUT);
    }

    @Test
    public void when_clientSubmitsSet_subscriberReceivesOriginalCommand() {
        SharedNumberSignal numberSignal = new SharedNumberSignal(1.0);
        InternalSignal internalSignal = new InternalSignal(numberSignal,
                mapper);
        ObjectNode setCommand = new ObjectNode(mapper.getNodeFactory())
                .put("commandId", Id.random().asBase64())
                .put("targetNodeId", numberSignal.id().asBase64())
                .put("@type", "set").put("value", 42);

        StepVerifier
                .create(internalSignal.subscribe(CLIENT_SIGNAL_ID_1)
                        .publishOn(Schedulers.parallel()))
                .consumeNextWith(event -> assertSnapshot(event, 1.0))
                .then(() -> internalSignal.submit(CLIENT_SIGNAL_ID_1,
                        setCommand))
                .consumeNextWith(event -> {
                    assertEquals(setCommand.get("commandId").asText(),
                            event.get("commandId").asText());
                    assertEquals("set", event.get("@type").asText());
                    assertEquals(42.0, event.get("value").asDouble(), 0.0);
                }).thenCancel().verify(VERIFY_TIMEOUT);
    }

    @Test
    public void when_noSubscriberOrSubscriptionCancelled_setDoesNotEmit() {
        SharedNumberSignal numberSignal = new SharedNumberSignal(1.0);
        InternalSignal internalSignal = new InternalSignal(numberSignal,
                mapper);

        numberSignal.set(2.0);

        List<JsonNode> events = new ArrayList<>();
        StepVerifier
                .create(internalSignal.subscribe(CLIENT_SIGNAL_ID_1)
                        .doOnNext(events::add))
                .consumeNextWith(event -> assertSnapshot(event, 2.0))
                .thenCancel().verify(VERIFY_TIMEOUT);

        numberSignal.set(3.0);
        assertEquals(1, events.size());

        StepVerifier.create(internalSignal.subscribe(CLIENT_SIGNAL_ID_1))
                .consumeNextWith(event -> assertSnapshot(event, 3.0))
                .thenCancel().verify(VERIFY_TIMEOUT);
    }

    private static void assertSnapshot(JsonNode event, double value) {
        assertNotNull(event);
        assertEquals("snapshot", event.get("@type").asText());
        assertEquals(value, event.get("nodes").get("").get("value").asDouble(),
                0.0);
    }

    private static void assertSnapshotFor(Tuple2<String, JsonNode> event,
            String clientSignalId, double value) {
        assertEquals(clientSignalId, event.getT1());
        assertSnapshot(event.getT2(), value);
    }

    private static void assertServerSetReachedBoth(
            List<Tuple2<String, JsonNode>> events, double value) {
        Set<String> clients = new HashSet<>();
        for (Tuple2<String, JsonNode> event : events) {
            assertServerSet(event.getT2(), value);
            clients.add(event.getT1());
        }
        assertEquals(Set.of(CLIENT_SIGNAL_ID_1, CLIENT_SIGNAL_ID_2), clients);
    }

    private static void assertServerSet(JsonNode command, double value) {
        assertNotNull(command);
        assertTrue(command.isObject());
        assertEquals("set", command.get("@type").asText());
        assertTrue(command.has("commandId"));
        assertTrue(command.get("commandId").isTextual());
        assertFalse(command.get("commandId").asText().isEmpty());
        assertEquals(value, command.get("value").asDouble(), 0.0);
    }
}
