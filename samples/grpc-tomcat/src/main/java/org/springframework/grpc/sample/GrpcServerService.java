package org.springframework.grpc.sample;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.grpc.sample.proto.HelloReply;
import org.springframework.grpc.sample.proto.HelloRequest;
import org.springframework.grpc.sample.proto.SimpleGrpc;
import org.springframework.grpc.web.util.MultiValueObserver;
import org.springframework.grpc.web.util.SingleValueObserver;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;

@Service
@RestController
public class GrpcServerService extends SimpleGrpc.SimpleImplBase {

	private static Log log = LogFactory.getLog(GrpcServerService.class);

	@Value("${stream.count:10}")
	int COUNT = 0;

	@Override
	public void sayHello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
		log.info("Hello " + req.getName());
		HelloReply reply = HelloReply.newBuilder().setMessage("Hello ==> " + req.getName()).build();
		responseObserver.onNext(reply);
		responseObserver.onCompleted();
	}

	@Override
	public void streamHello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
		log.info("Hello " + req.getName());
		int count = 0;
		while (count < COUNT) {
			HelloReply reply = HelloReply.newBuilder().setMessage("Hello(" + count + ") ==> " + req.getName()).build();
			responseObserver.onNext(reply);
			count++;
			try {
				Thread.sleep(1000L);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				responseObserver.onError(e);
				return;
			}
		}
		responseObserver.onCompleted();
	}

	@PostMapping(path = "Json/SayHello", produces = "application/json")
	public HelloReply sayHello(@RequestBody HelloRequest req) {
		SingleValueObserver<HelloReply> observer = new SingleValueObserver<>();
		sayHello(req, observer);
		return observer.getValue();
	}

	@PostMapping(path = "Json/StreamHello", produces = "application/x-ndjson")
	public Flux<HelloReply> stream(@RequestBody HelloRequest req) {
		MultiValueObserver<HelloReply> observer = new MultiValueObserver<>();
		streamHello(req, observer);
		return observer.getValue();
	}

}