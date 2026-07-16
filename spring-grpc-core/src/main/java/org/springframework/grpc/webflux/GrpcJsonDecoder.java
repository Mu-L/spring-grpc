/*
 * Copyright 2024-present the original author or authors.
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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;

import org.springframework.core.ResolvableType;
import org.springframework.core.codec.Decoder;
import org.springframework.core.codec.DecodingException;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.MediaType;
import org.springframework.util.ConcurrentReferenceHashMap;
import org.springframework.util.MimeType;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * WebFlux decoder for gRPC protocol buffer messages.
 * <p>
 * This decoder extends {@link GrpcCodecSupport} and implements {@link Decoder} to provide
 * decoding capabilities for gRPC messages in Spring WebFlux reactive streams, converting
 * byte streams to protocol buffer {@link Message} objects.
 *
 * @author Dave Syer
 * @since 1.0.0
 */
public class GrpcJsonDecoder extends GrpcCodecSupport implements Decoder<Message> {

	/** The default max size for aggregating messages. */
	protected static final int DEFAULT_MESSAGE_MAX_SIZE = 256 * 1024;

	private static final ConcurrentMap<Class<?>, Method> methodCache = new ConcurrentReferenceHashMap<>();

	private static final MimeType[] MIME_TYPES = new MimeType[] { MediaType.APPLICATION_JSON,
			new MediaType("application", "*+json"), MediaType.APPLICATION_NDJSON,
			new MediaType("application", "*+ndjson"), };

	public GrpcJsonDecoder() {
		super(MIME_TYPES);
	}

	public GrpcJsonDecoder(MimeType[] mimeTypes) {
		super(mimeTypes);
	}

	private int maxMessageSize = DEFAULT_MESSAGE_MAX_SIZE;

	/**
	 * The max size allowed per message.
	 * <p>
	 * By default, this is set to 256K.
	 * @param maxMessageSize the max size per message, or -1 for unlimited
	 */
	public void setMaxMessageSize(int maxMessageSize) {
		this.maxMessageSize = maxMessageSize;
	}

	/**
	 * Return the {@link #setMaxMessageSize configured} message size limit.
	 * @return the max size per message, or -1 for unlimited
	 */
	public int getMaxMessageSize() {
		return this.maxMessageSize;
	}

	@Override
	public boolean canDecode(ResolvableType elementType, @Nullable MimeType mimeType) {
		return Message.class.isAssignableFrom(elementType.toClass()) && supportsMimeType(mimeType);
	}

	@Override
	public Flux<Message> decode(Publisher<DataBuffer> inputStream, ResolvableType elementType,
			@Nullable MimeType mimeType, @Nullable Map<String, Object> hints) {

		MessageDecoderFunction decoderFunction = new MessageDecoderFunction(elementType, this.maxMessageSize);

		return (Flux<Message>) Flux.from(inputStream).flatMapIterable(decoderFunction);
	}

	@Override
	public Mono<Message> decodeToMono(Publisher<DataBuffer> inputStream, ResolvableType elementType,
			@Nullable MimeType mimeType, @Nullable Map<String, Object> hints) {

		return DataBufferUtils.join(inputStream, this.maxMessageSize)
			.map(dataBuffer -> decode(dataBuffer, elementType, mimeType, hints));
	}

	@Override
	public Message decode(DataBuffer dataBuffer, ResolvableType targetType, @Nullable MimeType mimeType,
			@Nullable Map<String, Object> hints) throws DecodingException {

		try {
			Message.Builder builder = getMessageBuilder(targetType.toClass());
			JsonFormat.parser()
				.merge(new InputStreamReader(dataBuffer.asInputStream(), StandardCharsets.UTF_8), builder);
			return builder.build();
		}
		catch (IOException ex) {
			throw new DecodingException("I/O error while parsing input stream", ex);
		}
		catch (Exception ex) {
			throw new DecodingException("Could not read Protobuf message: " + ex.getMessage(), ex);
		}
		finally {
			DataBufferUtils.release(dataBuffer);
		}
	}

	/**
	 * Create a new {@code Message.Builder} instance for the given class.
	 * @param clazz the class to create a builder for
	 * @return a new {@code Message.Builder} instance
	 * @throws Exception if the class does not have a valid {@code newBuilder()} method or
	 * if the method cannot be invoked
	 */
	private static Message.Builder getMessageBuilder(Class<?> clazz) throws Exception {
		Method method = methodCache.get(clazz);
		if (method == null) {
			method = clazz.getMethod("newBuilder");
			methodCache.put(clazz, method);
		}
		return (Message.Builder) method.invoke(clazz);
	}

	@Override
	public List<MimeType> getDecodableMimeTypes() {
		return getMimeTypes();
	}

	private class MessageDecoderFunction implements Function<DataBuffer, Iterable<? extends Message>> {

		private final ResolvableType elementType;

		private final int maxMessageSize;

		private final StringBuilder buffer = new StringBuilder();

		MessageDecoderFunction(ResolvableType elementType, int maxMessageSize) {
			this.elementType = elementType;
			this.maxMessageSize = maxMessageSize;
		}

		@Override
		public Iterable<? extends Message> apply(DataBuffer input) {
			// start tracks how many leading characters of this.buffer have been
			// processed and are safe to discard. It is declared outside the try block
			// so the finally clause can always clean up the consumed portion, even when
			// an exception is thrown mid-processing.
			int start = 0;
			try {
				List<Message> messages = new ArrayList<>();
				try (java.io.InputStream is = input.asInputStream()) {
					this.buffer.append(new String(is.readAllBytes(), StandardCharsets.UTF_8));
				}
				while (start < this.buffer.length()) {
					while (start < this.buffer.length() && Character.isWhitespace(this.buffer.charAt(start))) {
						start++;
					}
					if (start >= this.buffer.length()) {
						break;
					}
					if (this.maxMessageSize > 0 && this.buffer.length() - start > this.maxMessageSize) {
						throw new DataBufferLimitException(
								"Buffer size exceeds configured limit (" + this.maxMessageSize + ")");
					}
					String remaining = this.buffer.substring(start);
					// CountingReader intentionally returns 1 character per read() call so
					// that JsonReader cannot buffer past the end of the current JSON
					// value.
					// This makes getPosition() an accurate boundary marker but means
					// skipValue() performs O(n) individual reads. Replacing this with a
					// buffered approach would require a custom JSON boundary scanner.
					CountingReader countingReader = new CountingReader(remaining);
					try (JsonReader jsonReader = new JsonReader(countingReader)) {
						try {
							if (jsonReader.peek() == JsonToken.END_DOCUMENT) {
								break;
							}
							jsonReader.skipValue();
						}
						catch (EOFException e) {
							// Incomplete JSON object, wait for more data
							break;
						}
						catch (IOException e) {
							// Malformed JSON: discard the entire buffer so we do not
							// waste CPU re-parsing the same invalid bytes on every
							// subsequent DataBuffer delivery.
							start = this.buffer.length();
							throw new DecodingException("Malformed JSON in stream", e);
						}
					}
					int consumed = countingReader.getPosition();
					// Advance start before merge so that structurally-valid-but-invalid
					// protobuf JSON is also discarded rather than retried indefinitely.
					String json = this.buffer.substring(start, start + consumed);
					start += consumed;
					Message.Builder builder = getMessageBuilder(this.elementType.toClass());
					JsonFormat.parser().merge(new StringReader(json), builder);
					messages.add(builder.build());
				}
				return messages;
			}
			catch (DecodingException ex) {
				throw ex;
			}
			catch (IOException ex) {
				throw new DecodingException("I/O error while parsing input stream", ex);
			}
			catch (Exception ex) {
				throw new DecodingException("Could not read Protobuf message: " + ex.getMessage(), ex);
			}
			finally {
				// Always discard the processed portion of the buffer, including on
				// exception paths. Without this, malformed input would cause the buffer
				// to grow on every call until maxMessageSize is reached, with each call
				// wastefully re-parsing already-rejected bytes from position 0.
				this.buffer.delete(0, start);
				DataBufferUtils.release(input);
			}
		}

		/**
		 * A {@link Reader} that returns one character at a time from a String. This
		 * prevents {@link JsonReader} from buffering past the end of the current JSON
		 * object, so {@link #getPosition()} accurately reflects how many characters of
		 * the input have been logically consumed.
		 */
		private static class CountingReader extends Reader {

			private final String content;

			private int pos = 0;

			CountingReader(String content) {
				this.content = content;
			}

			@Override
			public int read(char[] cbuf, int off, int len) {
				if (this.pos >= this.content.length()) {
					return -1;
				}
				cbuf[off] = this.content.charAt(this.pos++);
				return 1;
			}

			int getPosition() {
				return this.pos;
			}

			@Override
			public void close() {
			}

		}

	}

}
