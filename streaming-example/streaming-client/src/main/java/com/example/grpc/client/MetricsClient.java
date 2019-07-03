/*
 * Copyright 2016 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.grpc.client;

import com.example.server.streaming.MetricsServiceGrpc;
import com.example.server.streaming.StreamingExample;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by rayt on 5/16/16.
 */
public class MetricsClient {

    public static void main(String[] args) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext().build();
        MetricsServiceGrpc.MetricsServiceStub stub = MetricsServiceGrpc.newStub(channel);
        final CountDownLatch finishLatch = new CountDownLatch(1);

        final StreamObserver<StreamingExample.Average> responseObserver = new StreamObserver<StreamingExample.Average>() {
            @Override
            public void onNext(StreamingExample.Average value) {
                System.out.println("Average: " + value.getVal());
            }

            @Override
            public void onError(Throwable t) {
                Status status = Status.fromThrowable(t);
                System.out.println("Request Failed: " + status);
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                System.out.println("Finished request");
                finishLatch.countDown();
            }
        };

        final StreamObserver<StreamingExample.Metric> requestObserver = stub.collect(responseObserver);
        try {
            for (Long l : Arrays.asList(1L, 2L, 3L, 4L)) {
                StreamingExample.Metric metric = StreamingExample.Metric.newBuilder().setMetric(l).build();
                requestObserver.onNext(metric);
                if (finishLatch.getCount() == 0) {
                    // RPC completed or errored before we finished sending.
                    // Sending further requests won't error, but they will just be thrown away.
                    return;
                }
            }
            requestObserver.onCompleted();
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }

        System.out.println("Latch count " + finishLatch.getCount());
        finishLatch.await(5, TimeUnit.SECONDS);
        System.out.println("Latch count " + finishLatch.getCount());
    }
}
