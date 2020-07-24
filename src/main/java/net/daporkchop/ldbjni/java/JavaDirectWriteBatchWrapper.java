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
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.ldbjni.direct.DirectWriteBatch;
import org.iq80.leveldb.WriteBatch;

import java.io.IOException;

import static net.daporkchop.ldbjni.java.JavaDirectDBWrapper.toArray;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
final class JavaDirectWriteBatchWrapper implements DirectWriteBatch {
    @NonNull
    protected final WriteBatch delegate;

    @Override
    public DirectWriteBatch put(@NonNull byte[] key, @NonNull byte[] value) {
        this.delegate.put(key, value);
        return this;
    }

    @Override
    public DirectWriteBatch put(@NonNull ByteBuf key, @NonNull ByteBuf value) {
        this.delegate.put(toArray(key), toArray(value));
        return this;
    }

    @Override
    public DirectWriteBatch delete(@NonNull byte[] key) {
        this.delegate.delete(key);
        return this;
    }

    @Override
    public DirectWriteBatch delete(@NonNull ByteBuf key) {
        this.delegate.delete(toArray(key));
        return this;
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }
}
