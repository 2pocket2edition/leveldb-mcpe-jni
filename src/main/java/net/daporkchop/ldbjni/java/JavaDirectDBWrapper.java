/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2020 DaPorkchop_
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * Any persons and/or organizations using this software must include the above copyright notice and this permission notice,
 * provide sufficient credit to the original authors of the project (IE: DaPorkchop_), as well as provide a link to the original project.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

package net.daporkchop.ldbjni.java;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.ldbjni.direct.DirectDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import java.io.IOException;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
final class JavaDirectDBWrapper implements DirectDB {
    private static byte[] toArray(@NonNull ByteBuf buf) {
        byte[] arr = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), arr);
        return arr;
    }

    @NonNull
    protected final DB delegate;

    @Override
    public byte[] get(@NonNull byte[] key) throws DBException {
        return this.delegate.get(key);
    }

    @Override
    public ByteBuf get(@NonNull ByteBuf key) throws DBException {
        return Unpooled.wrappedBuffer(this.get(toArray(key)));
    }

    @Override
    public byte[] get(@NonNull byte[] key, @NonNull ReadOptions options) throws DBException {
        return this.delegate.get(key, options);
    }

    @Override
    public ByteBuf get(@NonNull ByteBuf key, @NonNull ReadOptions options) throws DBException {
        return Unpooled.wrappedBuffer(this.get(toArray(key), options));
    }

    @Override
    public void put(@NonNull byte[] key, @NonNull byte[] value) throws DBException {
        this.delegate.put(key, value);
    }

    @Override
    public void put(@NonNull ByteBuf key, @NonNull ByteBuf value) throws DBException {
        this.put(toArray(key), toArray(value));
    }

    @Override
    public Snapshot put(@NonNull byte[] key, @NonNull byte[] value, @NonNull WriteOptions options) throws DBException {
        return this.delegate.put(key, value, options);
    }

    @Override
    public Snapshot put(@NonNull ByteBuf key, @NonNull ByteBuf value, @NonNull WriteOptions options) throws DBException {
        return this.put(toArray(key), toArray(value), options);
    }

    @Override
    public void delete(@NonNull byte[] key) throws DBException {
        this.delegate.delete(key);
    }

    @Override
    public void delete(@NonNull ByteBuf key) throws DBException {
        this.delete(toArray(key));
    }

    @Override
    public Snapshot delete(@NonNull byte[] key, @NonNull WriteOptions options) throws DBException {
        return this.delegate.delete(key, options);
    }

    @Override
    public Snapshot delete(@NonNull ByteBuf key, @NonNull WriteOptions options) throws DBException {
        return this.delete(toArray(key),options);
    }

    @Override
    public DBIterator iterator() {
        return this.delegate.iterator();
    }

    @Override
    public DBIterator iterator(@NonNull ReadOptions options) {
        return this.delegate.iterator(options);
    }

    @Override
    public WriteBatch createWriteBatch() {
        return this.delegate.createWriteBatch();
    }

    @Override
    public void write(@NonNull WriteBatch updates) throws DBException {
        this.delegate.write(updates);
    }

    @Override
    public Snapshot write(@NonNull WriteBatch updates, @NonNull WriteOptions options) throws DBException {
        return this.delegate.write(updates, options);
    }

    @Override
    public long[] getApproximateSizes(@NonNull Range... ranges) {
        return this.delegate.getApproximateSizes(ranges);
    }

    @Override
    public String getProperty(@NonNull String name) {
        return this.delegate.getProperty(name);
    }

    @Override
    public Snapshot getSnapshot() {
        return this.delegate.getSnapshot();
    }

    @Override
    public void compactRange(byte[] begin, byte[] end) throws DBException {
        this.delegate.compactRange(begin, end);
    }

    @Override
    public void suspendCompactions() throws InterruptedException {
        this.delegate.suspendCompactions();
    }

    @Override
    public void resumeCompactions() {
        this.delegate.resumeCompactions();
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }
}
