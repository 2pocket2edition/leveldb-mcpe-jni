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

package net.daporkchop.ldbjni.direct;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import java.io.IOException;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Extension of {@link DB} which allows use of direct buffers instead of {@code byte[]}s.
 *
 * @author DaPorkchop_
 */
public interface DirectDB extends DB {
    @Override
    byte[] get(@NonNull byte[] key) throws DBException;

    ByteBuf get(@NonNull ByteBuf key) throws DBException;

    default ByteBuf getZeroCopy(@NonNull ByteBuf key) throws IOException    {
        return this.get(key);
    }

    @Override
    byte[] get(@NonNull byte[] key, @NonNull ReadOptions options) throws DBException;

    ByteBuf get(@NonNull ByteBuf key, @NonNull ReadOptions options) throws DBException;

    default ByteBuf getZeroCopy(@NonNull ByteBuf key, @NonNull ReadOptions options) throws IOException  {
        return this.get(key, options);
    }

    @Override
    void put(@NonNull byte[] key, @NonNull byte[] value) throws DBException;

    void put(@NonNull ByteBuf key, @NonNull ByteBuf value) throws DBException;

    @Override
    Snapshot put(@NonNull byte[] key, @NonNull byte[] value, @NonNull WriteOptions options) throws DBException;

    Snapshot put(@NonNull ByteBuf key, @NonNull ByteBuf value, @NonNull WriteOptions options) throws DBException;

    @Override
    void delete(@NonNull byte[] key) throws DBException;

    void delete(@NonNull ByteBuf key) throws DBException;

    @Override
    Snapshot delete(@NonNull byte[] key, @NonNull WriteOptions options) throws DBException;

    Snapshot delete(@NonNull ByteBuf key, @NonNull WriteOptions options) throws DBException;

    @Override
    DBIterator iterator();

    @Override
    DBIterator iterator(@NonNull ReadOptions options);

    @Override
    WriteBatch createWriteBatch();

    @Override
    void write(@NonNull WriteBatch updates) throws DBException;

    @Override
    Snapshot write(@NonNull WriteBatch updates, @NonNull WriteOptions options) throws DBException;

    @Override
    long[] getApproximateSizes(@NonNull Range... ranges);

    @Override
    String getProperty(@NonNull String name);

    @Override
    Snapshot getSnapshot();

    @Override
    void compactRange(byte[] begin, byte[] end) throws DBException;

    @Override
    void suspendCompactions() throws InterruptedException;

    @Override
    void resumeCompactions();

    @Override
    void close() throws IOException;
}
