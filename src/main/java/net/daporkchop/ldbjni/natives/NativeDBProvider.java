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

package net.daporkchop.ldbjni.natives;

import lombok.NonNull;
import net.daporkchop.ldbjni.DBProvider;
import net.daporkchop.ldbjni.direct.DirectDB;
import net.daporkchop.lib.common.misc.file.PFiles;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import java.io.File;
import java.io.IOException;

/**
 * Native implementation of {@link DBProvider}.
 *
 * @author DaPorkchop_
 */
final class NativeDBProvider implements DBProvider {
    private static native void destroy0(String file,
                                        boolean create_if_missing, boolean error_if_exists, boolean paranoid_checks, int write_buffer_size,
                                        int max_open_files, int block_size, int block_restart_interval, int max_file_size, int compression, long cacheSize) throws IOException;

    private static native void repair0(String file,
                                       boolean create_if_missing, boolean error_if_exists, boolean paranoid_checks, int write_buffer_size,
                                       int max_open_files, int block_size, int block_restart_interval, int max_file_size, int compression, long cacheSize) throws IOException;

    @Override
    public boolean isNative() {
        return true;
    }

    @Override
    public DirectDB open(@NonNull File file, Options options) throws IOException {
        if (PFiles.checkDirectoryExists(file) && PFiles.checkFileExists(new File(file, "CURRENT")))  {
            //database has already been created
            File fixedFile = new File(file, "FIXED_MANIFEST");
            if (!PFiles.checkFileExists(fixedFile)) {
                //database manifest may not have been corrected by my horrible hack for the broken Java leveldb
                // (see https://github.com/NukkitX/leveldb/pull/1)
                try (DB db = Iq80DBFactory.factory.open(file, options)) {
                }
                PFiles.ensureFileExists(fixedFile);
            }
        }
        return new NativeDB(file, options);
    }

    @Override
    public void destroy(@NonNull File file, Options options) throws IOException {
        destroy0(
                file.getAbsoluteFile().getAbsolutePath(),
                options.createIfMissing(),
                options.errorIfExists(),
                options.paranoidChecks(),
                options.writeBufferSize(),
                options.maxOpenFiles(),
                options.blockSize(),
                options.blockRestartInterval(),
                -1, //not present in Options...
                options.compressionType().persistentId(),
                options.cacheSize());
    }

    @Override
    public void repair(@NonNull File file, Options options) throws IOException {
        repair0(
                file.getAbsoluteFile().getAbsolutePath(),
                options.createIfMissing(),
                options.errorIfExists(),
                options.paranoidChecks(),
                options.writeBufferSize(),
                options.maxOpenFiles(),
                options.blockSize(),
                options.blockRestartInterval(),
                -1, //not present in Options...
                options.compressionType().persistentId(),
                options.cacheSize());
    }
}
