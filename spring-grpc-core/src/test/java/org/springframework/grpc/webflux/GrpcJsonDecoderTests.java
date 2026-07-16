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

package org.springframework.grpc.webflux;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.core.ResolvableType;
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

	private DataBuffer toDataBuffer(String content) {
		byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
		DataBuffer buffer = bufferFactory.allocateBuffer(bytes.length);
		buffer.write(bytes);
		return buffer;
	}

}
