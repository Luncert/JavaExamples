package org.luncert.javaexamples.archivedatabase.export.repo.impl;

import io.netty.buffer.ByteBuf;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

@Slf4j
@Repository
@RequiredArgsConstructor
public class TransmissionCustomRepo {

    private final JdbcTemplate jdbcTemplate;

    @PostConstruct
    public void init() {
        jdbcTemplate.setFetchSize(1);
    }

    public void exportToByteArrayOutputStream(long startInclusive, long endExclusive, ByteArrayOutputStream byteBuffer) {
        Integer exported = exportTo(startInclusive, endExclusive, new ExportingOutputStream() {
            @Override
            void ensureWritable(int size) {
            }

            @Override
            public void write(int b) {
                byteBuffer.write((byte) b);
            }
        });
        log.info("exported {} from {} to {}, used {}", exported, startInclusive, endExclusive, byteBuffer.size());
    }

    public void exportToByteBuf(long startInclusive, long endExclusive, ByteBuf byteBuf) {
        Integer exported = exportTo(startInclusive, endExclusive, new ExportingOutputStream() {
            @Override
            void ensureWritable(int size) {
                byteBuf.ensureWritable(size);
            }

            @Override
            public void write(int b) {
                byteBuf.writeByte(b);
            }
        });
        log.info("exported {} from {} to {}, used {}", exported, startInclusive, endExclusive, byteBuf.arrayOffset());
    }

    private Integer exportTo(long startInclusive, long endExclusive, ExportingOutputStream output) {
        final int expectedResultSetSize = (int) (endExclusive - startInclusive);
        try {
            return jdbcTemplate.query(
                    "select * from TRANSMISSION where ID >= " + startInclusive + " and ID < " + endExclusive,
                    rset -> {
                        int i = 0;
                        while (rset.next()) {
                            StringBuilder builder = new StringBuilder();
                            builder.append(rset.getString(1)).append(',');
                            builder.append(rset.getString(2)).append(',');
                            builder.append(rset.getString(3)).append('\n');
                            try {
                                output.ensureWritable(builder.length() * (expectedResultSetSize - i) / 2);
                                output.write(builder.toString().getBytes());
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            i++;
                        }
                        return i;
                    });
        } catch (Exception e) {
            log.error("exception", e);
            return 0;
        }
    }

    private static abstract class ExportingOutputStream extends OutputStream {

        abstract void ensureWritable(int size);
    }
}
