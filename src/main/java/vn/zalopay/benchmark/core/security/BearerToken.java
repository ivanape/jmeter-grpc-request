package vn.zalopay.benchmark.core.security;

import java.util.concurrent.Executor;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

public class BearerToken extends CallCredentials {

    private String value;

    public static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY = Metadata.Key
        .of("Authorization", ASCII_STRING_MARSHALLER);

    public BearerToken(String value) {
        this.value = value;
    }

    @Override
    public void applyRequestMetadata(RequestInfo requestInfo, Executor executor, MetadataApplier metadataApplier) {
        executor.execute(() -> {
            try {
                Metadata headers = new Metadata();
                headers.put(AUTHORIZATION_METADATA_KEY, String.format("Bearer %s",value));
                metadataApplier.apply(headers);
            } catch (Exception e) {
                metadataApplier.fail(Status.UNAUTHENTICATED.withCause(e));
            }
        });
    }

    @Override
    public void thisUsesUnstableApi() {
        // noop
    }

}