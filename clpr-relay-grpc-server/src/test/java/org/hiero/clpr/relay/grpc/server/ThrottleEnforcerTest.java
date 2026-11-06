// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprConfigUpdate;
import com.hedera.hapi.node.state.clpr.ClprControlMessage;
import com.hedera.hapi.node.state.clpr.ClprMessage;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageReply;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.hiero.clpr.relay.core.ThrottleEnforcer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ThrottleEnforcerTest {

    /**
     * Build a Data Message whose user payload (the spec's {@code MaxMessagePayloadBytes}-bound field) is exactly
     * {@code payloadBytes} long.
     */
    private static ClprMessagePayload messageOfPayloadSize(final int payloadBytes) {
        return ClprMessagePayload.newBuilder()
                .message(ClprMessage.newBuilder()
                        .messageData(Bytes.wrap(new byte[payloadBytes]))
                        .build())
                .build();
    }

    /**
     * Build a Response Message whose user payload ({@code message_reply_data}) is exactly {@code payloadBytes} long.
     */
    private static ClprMessagePayload replyOfPayloadSize(final int payloadBytes) {
        return ClprMessagePayload.newBuilder()
                .messageReply(ClprMessageReply.newBuilder()
                        .messageReplyData(Bytes.wrap(new byte[payloadBytes]))
                        .build())
                .build();
    }

    /**
     * Build a Control Message — protocol-defined structure, not a user payload, so {@code MaxMessagePayloadBytes} does
     * not apply to it per spec §3.1.1.
     */
    private static ClprMessagePayload controlMessage() {
        return ClprMessagePayload.newBuilder()
                .control(ClprControlMessage.newBuilder()
                        .configUpdate(ClprConfigUpdate.newBuilder().build())
                        .build())
                .build();
    }

    /**
     * Spec §3.1.1 — {@code MaxMessagePayloadBytes} caps the user-data payload ({@code ClprMessage.message_data} /
     * {@code ClprMessageReply.message_reply_data}); the protobuf-encoded {@code ClprMessagePayload} wrapper is NOT
     * counted. Control messages are protocol-defined structures, so the user-set limit doesn't apply to them.
     */
    private static Stream<Arguments> maxMessagePayloadBytesCases() {
        return Stream.of(
                // Data Message: at-limit accepted, over-limit rejected
                Arguments.of("data: under limit", messageOfPayloadSize(128), 256, true),
                Arguments.of("data: at limit (inclusive)", messageOfPayloadSize(256), 256, true),
                Arguments.of("data: over limit", messageOfPayloadSize(257), 256, false),
                // Response Message: same cap applies to message_reply_data
                Arguments.of("reply: at limit", replyOfPayloadSize(256), 256, true),
                Arguments.of("reply: over limit", replyOfPayloadSize(257), 256, false),
                // Control Message: exempt — passes even when limit is tiny
                Arguments.of("control: exempt from user-payload limit", controlMessage(), 1, true));
    }

    @Test
    void rejectsBundleExceedingMaxSyncBytes() {
        var enforcer = new ThrottleEnforcer(1024L, 0, 0);
        assertThat(enforcer.shouldAccept(Bytes.wrap(new byte[512]), List.of())).isTrue();
        assertThat(enforcer.shouldAccept(Bytes.wrap(new byte[1024]), List.of())).isTrue(); // boundary inclusive
        assertThat(enforcer.shouldAccept(Bytes.wrap(new byte[1025]), List.of())).isFalse();
    }

    @Test
    void bundleSizeUnthrottledWhenLimitZero() {
        var enforcer = new ThrottleEnforcer(0L, 0, 0);
        assertThat(enforcer.shouldAccept(Bytes.wrap(new byte[10_000]), List.of()))
                .isTrue();
    }

    @Test
    void rejectsBundleExceedingMaxMessagesPerBundle() {
        var enforcer = new ThrottleEnforcer(0L, 4, 0);
        assertThat(enforcer.shouldAccept(Bytes.EMPTY, Collections.nCopies(4, ClprMessagePayload.DEFAULT)))
                .isTrue();
        assertThat(enforcer.shouldAccept(Bytes.EMPTY, Collections.nCopies(5, ClprMessagePayload.DEFAULT)))
                .isFalse();
    }

    @Test
    void messageCountUnthrottledWhenLimitZero() {
        var enforcer = new ThrottleEnforcer(0L, 0, 0);
        final List<ClprMessagePayload> many = new ArrayList<>(Collections.nCopies(1000, ClprMessagePayload.DEFAULT));
        assertThat(enforcer.shouldAccept(Bytes.EMPTY, many)).isTrue();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("maxMessagePayloadBytesCases")
    void enforcesMaxMessagePayloadBytesPerSpec(
            final String description,
            final ClprMessagePayload message,
            final int maxMessagePayloadBytes,
            final boolean expected) {
        var enforcer = new ThrottleEnforcer(0L, 0, maxMessagePayloadBytes);
        assertThat(enforcer.shouldAccept(Bytes.EMPTY, List.of(message))).isEqualTo(expected);
    }

    @Test
    void payloadSizeUnthrottledWhenLimitZero() {
        var enforcer = new ThrottleEnforcer(0L, 0, 0);
        assertThat(enforcer.shouldAccept(Bytes.EMPTY, List.of(messageOfPayloadSize(10_000))))
                .isTrue();
    }
}
