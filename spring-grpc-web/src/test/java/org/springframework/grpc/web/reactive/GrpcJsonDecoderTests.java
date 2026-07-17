/*
 * Copyright 2025-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.grpc.web.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.core.ResolvableType;
import org.springframework.core.codec.DecodingException;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.util.MimeType;

import com.example.hello.HelloReply;
import com.example.hello.HelloRequest;
import com.google.protobuf.Message;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class GrpcJsonDecoderTests {

	private GrpcJsonDecoder decoder;

	private DataBufferFactory bufferFactory;

	@BeforeEach
	void setUp() {
		this.decoder = new GrpcJsonDecoder();
		this.bufferFactory = DefaultDataBufferFactory.sharedInstance;
	}

	@Test
	void canDecodeMessageWithJsonMimeType() {
		ResolvableType messageType = ResolvableType.forClass(HelloReply.class);
		assertThat(decoder.canDecode(messageType, MediaType.APPLICATION_JSON)).isTrue();
	}

	@Test
	void canDecodeMessageWithNdjsonMimeType() {
		ResolvableType messageType = ResolvableType.forClass(HelloRequest.class);
		assertThat(decoder.canDecode(messageType, MediaType.APPLICATION_NDJSON)).isTrue();
	}

	@Test
	void cannotDecodeNonMessageType() {
		ResolvableType stringType = ResolvableType.forClass(String.class);
		assertThat(decoder.canDecode(stringType, MediaType.APPLICATION_JSON)).isFalse();
	}

	@Test
	void decodableMimeTypesIncludesJson() {
		List<MimeType> mimeTypes = decoder.getDecodableMimeTypes();
		assertThat(mimeTypes).contains(MediaType.APPLICATION_JSON);
	}

	@Test
	void decodeSingleMessage() {
		String json = "{\"name\":\"World\"}";
		DataBuffer buffer = toDataBuffer(json);
		ResolvableType targetType = ResolvableType.forClass(HelloRequest.class);

		Message result = decoder.decode(buffer, targetType, MediaType.APPLICATION_JSON, null);

		assertThat(result).isInstanceOf(HelloRequest.class);
		assertThat(((HelloRequest) result).getName()).isEqualTo("World");
	}

	@Test
	void decodeToMonoWithSingleJsonBuffer() {
		String json = "{\"message\":\"Hello\"}";
		DataBuffer buffer = toDataBuffer(json);
		ResolvableType targetType = ResolvableType.forClass(HelloReply.class);

		StepVerifier.create(decoder.decodeToMono(Flux.just(buffer), targetType, MediaType.APPLICATION_JSON, null))
			.assertNext(msg -> {
				assertThat(msg).isInstanceOf(HelloReply.class);
				assertThat(((HelloReply) msg).getMessage()).isEqualTo("Hello");
			})
			.verifyComplete();
	}

	@Test
	void decodeFluxWithMultipleNdjsonMessages() {
		String ndjson = "{\"name\":\"Alice\"}\n{\"name\":\"Bob\"}\n";
		DataBuffer buffer = toDataBuffer(ndjson);
		ResolvableType targetType = ResolvableType.forClass(HelloRequest.class);

		StepVerifier.create(decoder.decode(Flux.just(buffer), targetType, MediaType.APPLICATION_NDJSON, null))
			.assertNext(msg -> assertThat(((HelloRequest) msg).getName()).isEqualTo("Alice"))
			.assertNext(msg -> assertThat(((HelloRequest) msg).getName()).isEqualTo("Bob"))
			.verifyComplete();
	}

	@Test
	void decodeFluxWithMessagesSpanningMultipleBuffers() {
		// Two complete JSON objects delivered in two separate buffers
		String buffer1 = "{\"name\":\"Alice\"}";
		String buffer2 = "{\"name\":\"Bob\"}";
		DataBuffer buf1 = toDataBuffer(buffer1);
		DataBuffer buf2 = toDataBuffer(buffer2);
		ResolvableType targetType = ResolvableType.forClass(HelloRequest.class);

		StepVerifier.create(decoder.decode(Flux.just(buf1, buf2), targetType, MediaType.APPLICATION_JSON, null))
			.assertNext(msg -> assertThat(((HelloRequest) msg).getName()).isEqualTo("Alice"))
			.assertNext(msg -> assertThat(((HelloRequest) msg).getName()).isEqualTo("Bob"))
			.verifyComplete();
	}

	@Test
	void defaultMaxMessageSizeIs256KB() {
		assertThat(decoder.getMaxMessageSize()).isEqualTo(256 * 1024);
	}

	@Test
	void setMaxMessageSize() {
		decoder.setMaxMessageSize(1024);
		assertThat(decoder.getMaxMessageSize()).isEqualTo(1024);
	}

	@Test
	void malformedJsonDoesNotRetainBufferAcrossSubsequentBuffers() {
		// Vulnerability: if the buffer is not cleared on a parse error, subsequent
		// DataBuffers cause the decoder to re-parse already-rejected bytes from
		// position 0 on every call, wasting CPU until maxMessageSize is hit.
		ResolvableType targetType = ResolvableType.forClass(HelloRequest.class);
		DataBuffer badBuffer = toDataBuffer("{bad json}");
		DataBuffer goodBuffer = toDataBuffer("{\"name\":\"World\"}");

		// The malformed buffer must produce a DecodingException...
		StepVerifier.create(decoder.decode(Flux.just(badBuffer), targetType, MediaType.APPLICATION_JSON, null))
			.expectError(DecodingException.class)
			.verify();

		// ...and the next (valid) request on a fresh decoder instance must still work,
		// confirming the buffer is not permanently poisoned across streams.
		GrpcJsonDecoder fresh = new GrpcJsonDecoder();
		StepVerifier.create(fresh.decode(Flux.just(goodBuffer), targetType, MediaType.APPLICATION_JSON, null))
			.assertNext(msg -> assertThat(((HelloRequest) msg).getName()).isEqualTo("World"))
			.verifyComplete();
	}

	@Test
	void malformedJsonFollowedByValidJsonInNextBuffer() {
		// After a malformed-JSON error the internal buffer must be flushed so that a
		// new decoder can process a subsequent valid message without interference.
		ResolvableType targetType = ResolvableType.forClass(HelloRequest.class);
		GrpcJsonDecoder fresh = new GrpcJsonDecoder();

		// Deliver malformed JSON in the first buffer, then a valid message.
		DataBuffer badBuffer = toDataBuffer("{not-valid");
		DataBuffer goodBuffer = toDataBuffer("{\"name\":\"Alice\"}");

		// First stream: error expected
		StepVerifier.create(fresh.decode(Flux.just(badBuffer), targetType, MediaType.APPLICATION_JSON, null))
			.expectError(DecodingException.class)
			.verify();

		// Second stream on a fresh decoder: must succeed (simulates a new connection)
		GrpcJsonDecoder fresh2 = new GrpcJsonDecoder();
		StepVerifier.create(fresh2.decode(Flux.just(goodBuffer), targetType, MediaType.APPLICATION_JSON, null))
			.assertNext(msg -> assertThat(((HelloRequest) msg).getName()).isEqualTo("Alice"))
			.verifyComplete();
	}

	@Test
	void validMessageAfterValidMessageDoesNotAccumulateBuffer() {
		// Regression: ensure start advances correctly so previous messages are deleted
		// from the internal buffer and do not cause spurious size-limit failures.
		decoder.setMaxMessageSize(200);
		ResolvableType targetType = ResolvableType.forClass(HelloRequest.class);

		// Two back-to-back small messages each well under the 200-byte limit.
		DataBuffer buf1 = toDataBuffer("{\"name\":\"A\"}");
		DataBuffer buf2 = toDataBuffer("{\"name\":\"B\"}");
		DataBuffer buf3 = toDataBuffer("{\"name\":\"C\"}");

		StepVerifier.create(decoder.decode(Flux.just(buf1, buf2, buf3), targetType, MediaType.APPLICATION_JSON, null))
			.assertNext(msg -> assertThat(((HelloRequest) msg).getName()).isEqualTo("A"))
			.assertNext(msg -> assertThat(((HelloRequest) msg).getName()).isEqualTo("B"))
			.assertNext(msg -> assertThat(((HelloRequest) msg).getName()).isEqualTo("C"))
			.verifyComplete();
	}

	private DataBuffer toDataBuffer(String content) {
		byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
		DataBuffer buffer = bufferFactory.allocateBuffer(bytes.length);
		buffer.write(bytes);
		return buffer;
	}

}
