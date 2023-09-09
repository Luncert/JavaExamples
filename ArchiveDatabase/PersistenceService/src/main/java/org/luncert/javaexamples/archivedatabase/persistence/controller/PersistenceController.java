package org.luncert.javaexamples.archivedatabase.persistence.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@RestController
public class PersistenceController {

    private String fileName;
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private ReentrantLock lock = new ReentrantLock();

    @GetMapping("/status")
    public boolean exists(@RequestParam String fileName) {
        lock.lock();
        try {
            return fileName.equals(this.fileName);
        } finally {
            lock.unlock();
        }
    }

    @PostMapping("/create")
    public void create(@RequestParam String fileName, @RequestBody byte[] body) throws IOException {
        lock.lock();
        try {
            this.fileName = fileName;
            outputStream.reset();
            outputStream.write(body);
        } finally {
            lock.unlock();
        }
    }

    @PutMapping("/append")
    public void append(@RequestParam String fileName, @RequestBody byte[] body) throws IOException {
        try {
            outputStream.write(body);
            log.info("append: {}/{}", body.length, outputStream.size());
        } finally {
            lock.unlock();
        }
    }

    @GetMapping(value = "/get", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<byte[]> get(@RequestParam String fileName, @RequestParam int offset, @RequestParam int length) {
        byte[] bytes = outputStream.toByteArray();
        int end = Math.min(offset + length, bytes.length);
        bytes = Arrays.copyOfRange(bytes, offset, end);
        return ResponseEntity.ok(bytes);
    }
}
