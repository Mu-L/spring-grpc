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

package org.springframework.grpc.web.servlet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.http.MediaType;
import org.springframework.mock.http.MockHttpInputMessage;
import org.springframework.mock.http.MockHttpOutputMessage;

import com.example.hello.HelloReply;
import com.example.hello.HelloRequest;

class GrpcJsonHttpMessageConverterTests {

	private GrpcJsonHttpMessageConverter converter;

	@BeforeEach
	void setUp() {
		this.converter = new GrpcJsonHttpMessageConverter();
	}

	@Test
	void supportsMessageSubclass() {
		assertThat(converter.getSupportedMediaTypes(HelloRequest.class)).contains(MediaType.APPLICATION_JSON);
	}

	@Test
	void doesNotSupportNonMessageClass() {
		assertThat(converter.getSupportedMediaTypes(String.class)).isEmpty();
	}

	@Test
	void canWriteApplicationJson() {
		assertThat(converter.canWrite(HelloReply.class, MediaType.APPLICATION_JSON)).isTrue();
	}

	@Test
	void canWriteNullMediaType() {
		// should default to supported type
		assertThat(converter.canWrite(HelloReply.class, null)).isTrue();
	}

	@Test
	void canReadApplicationJson() {
		assertThat(converter.canRead(HelloRequest.class, MediaType.APPLICATION_JSON)).isTrue();
	}

	@Test
	void readConvertsJsonToMessage() throws IOException {
		String json = "{\"name\":\"Alice\"}";
		MockHttpInputMessage inputMessage = new MockHttpInputMessage(json.getBytes(StandardCharsets.UTF_8));
		inputMessage.getHeaders().setContentType(MediaType.APPLICATION_JSON);

		HelloRequest result = (HelloRequest) converter.read(HelloRequest.class, inputMessage);

		assertThat(result.getName()).isEqualTo("Alice");
	}

	@Test
	void readUsesUtf8WhenCharsetNotSpecified() throws IOException {
		String json = "{\"name\":\"Ünïcödë\"}";
		MockHttpInputMessage inputMessage = new MockHttpInputMessage(json.getBytes(StandardCharsets.UTF_8));
		// No charset in content-type
		inputMessage.getHeaders().setContentType(MediaType.APPLICATION_JSON);

		HelloRequest result = (HelloRequest) converter.read(HelloRequest.class, inputMessage);

		assertThat(result.getName()).isEqualTo("Ünïcödë");
	}

	@Test
	void writeConvertsMessageToJson() throws IOException {
		HelloReply message = HelloReply.newBuilder().setMessage("Hello World").build();
		MockHttpOutputMessage outputMessage = new MockHttpOutputMessage();

		converter.write(message, MediaType.APPLICATION_JSON, outputMessage);

		String body = outputMessage.getBodyAsString(StandardCharsets.UTF_8);
		assertThat(body).isEqualTo("{\"message\":\"Hello World\"}");
	}

	@Test
	void writeEmptyMessageProducesEmptyObject() throws IOException {
		HelloReply message = HelloReply.newBuilder().build();
		MockHttpOutputMessage outputMessage = new MockHttpOutputMessage();

		converter.write(message, MediaType.APPLICATION_JSON, outputMessage);

		String body = outputMessage.getBodyAsString(StandardCharsets.UTF_8);
		assertThat(body).isEqualTo("{}");
	}

	@Test
	void writeSetsContentTypeHeader() throws IOException {
		HelloReply message = HelloReply.newBuilder().setMessage("Test").build();
		MockHttpOutputMessage outputMessage = new MockHttpOutputMessage();

		converter.write(message, MediaType.APPLICATION_JSON, outputMessage);

		MediaType contentType = outputMessage.getHeaders().getContentType();
		assertThat(contentType).isNotNull();
		assertThat(contentType.isCompatibleWith(MediaType.APPLICATION_JSON)).isTrue();
	}

	@Test
	void defaultCharsetIsUtf8() {
		assertThat(GrpcJsonHttpMessageConverter.DEFAULT_CHARSET).isEqualTo(StandardCharsets.UTF_8);
	}

}
