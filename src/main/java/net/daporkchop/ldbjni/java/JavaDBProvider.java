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

import lombok.NonNull;
import net.daporkchop.ldbjni.DBProvider;
import net.daporkchop.ldbjni.direct.DirectDB;
import net.daporkchop.lib.common.misc.file.PFiles;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import java.io.File;
import java.io.IOException;

/**
 * Dummy implementation of {@link DBProvider} that simply delegates to {@link Iq80DBFactory}.
 *
 * @author DaPorkchop_
 */
final class JavaDBProvider implements DBProvider {
    @Override
    public boolean isNative() {
        return false;
    }

    @Override
    public DirectDB open(@NonNull File file, Options options) throws IOException {
        if (PFiles.checkDirectoryExists(file) && PFiles.checkFileExists(new File(file, "CURRENT"))) {
            //database has already been created
            File fixedFile = new File(file, "FIXED_MANIFEST");
            if (PFiles.checkFileExists(fixedFile)) {
                //remove manifest fixed flag if it already exists, because we can't be certain that the manifest will contain the correct fileSize
                // parameters if a new table is created. this allows the fallback to java leveldb to be totally safe (although it is possible that
                // if the db is opened using java leveldb that isn't wrapped by this library it will be broken, but that's quite an obscure edge case)
                PFiles.rm(fixedFile);
            }
        }
        return new JavaDirectDBWrapper(Iq80DBFactory.factory.open(file, options));
    }

    @Override
    public void destroy(@NonNull File file, Options options) throws IOException {
        Iq80DBFactory.factory.destroy(file, options);
    }

    @Override
    public void repair(@NonNull File file, Options options) throws IOException {
        Iq80DBFactory.factory.repair(file, options);
    }
}
