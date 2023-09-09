package org.luncert.javaexamples.archivedatabase.export.service.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.luncert.javaexamples.archivedatabase.export.model.Transmission;
import org.luncert.javaexamples.archivedatabase.export.repo.ITransmissionRepo;
import org.luncert.javaexamples.archivedatabase.export.repo.impl.TransmissionCustomRepo;
import org.luncert.javaexamples.archivedatabase.export.service.IExportService;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import static org.asynchttpclient.Dsl.asyncHttpClient;

@Slf4j
@Service
@RequiredArgsConstructor
public class ExportServiceImpl implements IExportService {

    private final ITransmissionRepo transmissionRepo;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final TransmissionCustomRepo transmissionCustomRepo;

    private final PooledByteBufAllocator byteBufAllocator = new PooledByteBufAllocator(true);

    @PostConstruct
    public void init() {
        List<Transmission> transmissions = new ArrayList<>(10000);
        for (int i = 0; i < 10000; i++) {
            transmissions.add(new Transmission((long) i, "trans" + i, "payload" + i));
        }
        transmissionRepo.saveAll(transmissions);
        log.info("data inserted");
    }

    @Override
    public void exportDirectly() {
        long startAt = System.currentTimeMillis();
        long count = transmissionRepo.count();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        transmissionCustomRepo.exportToByteArrayOutputStream(0, count, out);
        try {
            upload(out);
        } catch (IOException | ExecutionException | InterruptedException e) {
            log.error("exception", e);
        }
        log.info("job used: {}ms", System.currentTimeMillis() - startAt);
    }

    @Override
    public void exportUsingDirectMemory() {
        long startAt = System.currentTimeMillis();
        long count = transmissionRepo.count();
        long partitionSize = count / 3 + 1;
        for (long i = 0; i < count; i += partitionSize) {
            final long start = i;
            final long end = Math.min(i + partitionSize, count);
            threadPoolExecutor.submit(() -> {
                try {
                    ByteBuf byteBuf = byteBufAllocator.heapBuffer();
                    transmissionCustomRepo.exportToByteBuf(start, end, byteBuf);
                    upload(byteBuf);
                    byteBuf.release();
                    log.info("task ({}-{}) used: {}ms", start, end, System.currentTimeMillis() - startAt);
                } catch (IOException | ExecutionException | InterruptedException e) {
                    log.error("exception", e);
                }
            });
        }
        transmissionRepo.findAll();
    }

    private void upload(ByteArrayOutputStream out) throws IOException, ExecutionException, InterruptedException {
        try (AsyncHttpClient c = asyncHttpClient()) {
            ListenableFuture<Response> future = c.prepareGet("http://localhost:8081/status?fileName=Transmission.csv").execute();
            Boolean exists = Boolean.valueOf(future.get().getResponseBody());
            if (!Boolean.TRUE.equals(exists)) {
                future = c.preparePost("http://localhost:8081/create?fileName=Transmission.csv").setBody("id,name,payload\n").execute();
                future.get();
            }

            c.preparePut("http://localhost:8081/append?fileName=Transmission.csv").setBody(out.toByteArray()).execute().get();
        }
    }

    private void upload(ByteBuf buf) throws IOException, ExecutionException, InterruptedException {
        try (AsyncHttpClient c = asyncHttpClient()) {
            ListenableFuture<Response> future = c.prepareGet("http://localhost:8081/status?fileName=Transmission.csv").execute();
            Boolean exists = Boolean.valueOf(future.get().getResponseBody());
            if (!Boolean.TRUE.equals(exists)) {
                future = c.preparePost("http://localhost:8081/create?fileName=Transmission.csv").setBody("id,name,payload\n").execute();
                future.get();
            }

            c.preparePut("http://localhost:8081/append?fileName=Transmission.csv").setBody(buf.nioBuffer()).execute().get();
        }
    }
}
