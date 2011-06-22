package com.github.cwilper.bigtrouble;

import com.github.cwilper.ttff.Source;

import java.io.IOException;
import java.io.InputStream;

class ChunkedInputStream extends InputStream {

    private final Source<byte[]> source;

    private byte[] chunk;
    private int offset;

    ChunkedInputStream(Source<byte[]> source) {
        this.source = source;
        chunk = getNext();
    }

    @Override
    public int read() throws IOException {
        if (chunk == null) {
            return -1;
        } else {
            int value = chunk[offset++];
            if (offset == chunk.length) {
                chunk = getNext();
            }
            return value;
        }
    }

    private byte[] getNext() {
        try {
            if (source.hasNext()) {
                offset = 0;
                return source.next();
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
