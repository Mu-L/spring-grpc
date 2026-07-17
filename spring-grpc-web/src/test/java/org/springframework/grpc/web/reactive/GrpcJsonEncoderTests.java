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
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.util.MimeType;

import com.example.hello.HelloReply;
import com.example.hello.HelloRequest;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class GrpcJsonEncoderTests {

	private GrpcJsonEncoder encoder;

	private DataBufferFactory bufferFactory;

	@BeforeEach
	void setUp() {
		this.encoder = new GrpcJsonEncoder();
		this.bufferFactory = DefaultDataBufferFactory.sharedInstance;
	}

	@Test
	void canEncodeMessageWithJsonMimeType() {
		ResolvableType messageType = ResolvableType.forClass(HelloReply.class);
		assertThat(encoder.canEncode(messageType, MediaType.APPLICATION_JSON)).isTrue();
	}

	@Test
	void canEncodeMessageWithNdjsonMimeType() {
		ResolvableType messageType = ResolvableType.forClass(HelloRequest.class);
		assertThat(encoder.canEncode(messageType, MediaType.APPLICATION_NDJSON)).isTrue();
	}

	@Test
	void cannotEncodeNonMessageType() {
		ResolvableType stringType = ResolvableType.forClass(String.class);
		assertThat(encoder.canEncode(stringType, MediaType.APPLICATION_JSON)).isFalse();
	}

	@Test
	void encodableMimeTypesIncludesJson() {
		List<MimeType> mimeTypes = encoder.getEncodableMimeTypes();
		assertThat(mimeTypes).contains(MediaType.APPLICATION_JSON);
	}

	@Test
	void encodeValueProducesValidJson() {
		HelloReply message = HelloReply.newBuilder().setMessage("Hello World").build();
		ResolvableType valueType = ResolvableType.forClass(HelloReply.class);

		DataBuffer buffer = encoder.encodeValue(message, bufferFactory, valueType, MediaType.APPLICATION_JSON, null);

		String json = buffer.toString(StandardCharsets.UTF_8);
		assertThat(json).isEqualTo("{\"message\":\"Hello World\"}");
	}

	@Test
	void encodeValueWithNdjsonAddsNewline() {
		HelloReply message = HelloReply.newBuilder().setMessage("Streaming").build();
		ResolvableType valueType = ResolvableType.forClass(HelloReply.class);

		DataBuffer buffer = encoder.encodeValue(message, bufferFactory, valueType, MediaType.APPLICATION_NDJSON, null);

		String result = buffer.toString(StandardCharsets.UTF_8);
		assertThat(result).endsWith("\n");
		assertThat(result).startsWith("{");
	}

	@Test
	void encodeFluxProducesMultipleBuffers() {
		HelloReply reply1 = HelloReply.newBuilder().setMessage("First").build();
		HelloReply reply2 = HelloReply.newBuilder().setMessage("Second").build();
		ResolvableType elementType = ResolvableType.forClass(HelloReply.class);

		Flux<DataBuffer> result = encoder.encode(Flux.just(reply1, reply2), bufferFactory, elementType,
				MediaType.APPLICATION_JSON, null);

		StepVerifier.create(result.map(buf -> buf.toString(StandardCharsets.UTF_8)))
			.expectNext("{\"message\":\"First\"}")
			.expectNext("{\"message\":\"Second\"}")
			.verifyComplete();
	}

	@Test
	void streamingMediaTypesContainsNdjson() {
		assertThat(encoder.getStreamingMediaTypes()).contains(MediaType.APPLICATION_NDJSON);
	}

	@Test
	void encodeEmptyStringFieldOmitted() {
		HelloReply message = HelloReply.newBuilder().build();
		ResolvableType valueType = ResolvableType.forClass(HelloReply.class);

		DataBuffer buffer = encoder.encodeValue(message, bufferFactory, valueType, MediaType.APPLICATION_JSON, null);

		String json = buffer.toString(StandardCharsets.UTF_8);
		// Proto3 default values are omitted by JsonFormat with
		// omittingInsignificantWhitespace
		assertThat(json).isEqualTo("{}");
	}

}
