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
import io.netty.buffer.ByteBufAllocator;
import lombok.NonNull;

/**
 * The type of buffer that the data was requested to be stored in.
 *
 * @author DaPorkchop_
 */
public enum BufType {
    DEFAULT {
        @Override
        public ByteBuf allocate(@NonNull ByteBufAllocator alloc, int size) {
            return alloc.buffer(size);
        }
    },
    IO {
        @Override
        public ByteBuf allocate(@NonNull ByteBufAllocator alloc, int size) {
            return alloc.ioBuffer(size);
        }
    },
    HEAP {
        @Override
        public ByteBuf allocate(@NonNull ByteBufAllocator alloc, int size) {
            return alloc.heapBuffer(size);
        }
    },
    DIRECT {
        @Override
        public ByteBuf allocate(@NonNull ByteBufAllocator alloc, int size) {
            return alloc.directBuffer(size);
        }
    };

    /**
     * Allocates a new {@link ByteBuf} with the given size.
     *
     * @param alloc the {@link ByteBufAllocator} to use
     * @param size  the size of the buffer to allocate
     * @return the allocated {@link ByteBuf}
     */
    public abstract ByteBuf allocate(@NonNull ByteBufAllocator alloc, int size);
}
