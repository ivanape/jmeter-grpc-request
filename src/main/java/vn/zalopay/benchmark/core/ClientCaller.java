package vn.zalopay.benchmark.core;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Attributes;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import vn.zalopay.benchmark.core.grpc.ChannelFactory;
import vn.zalopay.benchmark.core.grpc.DynamicGrpcClient;
import vn.zalopay.benchmark.core.io.MessageReader;
import vn.zalopay.benchmark.core.protobuf.ProtoMethodName;
import vn.zalopay.benchmark.core.protobuf.ProtocInvoker;
import vn.zalopay.benchmark.core.protobuf.ServiceResolver;
import io.grpc.CallOptions;
import io.grpc.Channel;
import vn.zalopay.benchmark.core.security.BearerToken;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class ClientCaller {
    private Descriptors.MethodDescriptor methodDescriptor;
    private JsonFormat.TypeRegistry registry;
    private HostAndPort hostAndPort;
    private DynamicGrpcClient dynamicClient;
    private ImmutableList<DynamicMessage> requestMessages;

    public ClientCaller(String HOST_PORT, String TEST_PROTO_FILES, String FULL_METHOD, boolean TLS) {
        this.init(HOST_PORT, TEST_PROTO_FILES, FULL_METHOD, TLS);
    }

    private void init(String HOST_PORT, String TEST_PROTO_FILES, String FULL_METHOD, boolean tls) {
        hostAndPort = HostAndPort.fromString(HOST_PORT);
        ProtoMethodName grpcMethodName =
                ProtoMethodName.parseFullGrpcMethodName(FULL_METHOD);

        ChannelFactory channelFactory = ChannelFactory.create();

        Channel channel;
        channel = channelFactory.createChannel(hostAndPort, tls);

        // Fetch the appropriate file descriptors for the service.
        final DescriptorProtos.FileDescriptorSet fileDescriptorSet;

        try {
            fileDescriptorSet = ProtocInvoker.forConfig(TEST_PROTO_FILES).invoke();
        } catch (Throwable t) {
            throw new RuntimeException("Unable to resolve service by invoking protoc", t);
        }

        // Set up the dynamic client and make the call.
        ServiceResolver serviceResolver = ServiceResolver.fromFileDescriptorSet(fileDescriptorSet);
        methodDescriptor = serviceResolver.resolveServiceMethod(grpcMethodName);

        dynamicClient = DynamicGrpcClient.create(methodDescriptor, channel);

        // This collects all known types into a registry for resolution of potential "Any" types.
        registry = JsonFormat.TypeRegistry.newBuilder()
                .add(serviceResolver.listMessageTypes())
                .build();
    }

    public String buildRequest(String pathReq, String jsonData) {
        Path REQUEST_FILE = Paths.get(pathReq);

        requestMessages =
                MessageReader.forFile(REQUEST_FILE, methodDescriptor.getInputType(), registry, jsonData).read();
        return requestMessages.get(0).toString();
    }

    public DynamicMessage call(long deadlineMs, String jwtToken) {
        DynamicMessage resp;
        try {
            resp = dynamicClient.blockingUnaryCall(requestMessages, callOptions(deadlineMs, jwtToken));
        } catch (Throwable t) {
            throw new RuntimeException("Caught exception while waiting for rpc", t);
        }
        return resp;
    }

    private static CallOptions callOptions(long deadlineMs, String jwtToken) {
        CallOptions result = CallOptions.DEFAULT;

        if (deadlineMs > 0) {
            result = result.withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
        }

        if (jwtToken != null && jwtToken.length() > 0){
            result = result.withCallCredentials(new BearerToken(jwtToken));
        }

        return result;
    }

}
