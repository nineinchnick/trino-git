/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.net.was.presto.git;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.internal.storage.file.PackFile;
import org.eclipse.jgit.internal.storage.file.PackIndex;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.eclipse.jgit.lib.Constants.OBJ_BLOB;

public class ObjectsRecordCursor
        implements RecordCursor
{
    private final List<GitColumnHandle> columnHandles;

    private final FileRepository fileRepo;
    private final Iterator<PackFile> packs;
    private Iterator<PackIndex.MutableEntry> entries;
    private ObjectId objectId;
    private ObjectLoader loader;

    private List<String> fields;

    public ObjectsRecordCursor(List<GitColumnHandle> columnHandles, Git repo)
    {
        this.columnHandles = columnHandles;

        fileRepo = (FileRepository) repo.getRepository();
        Collection<PackFile> packs = fileRepo.getObjectDatabase().getPacks();
        this.packs = packs.iterator();
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (packs == null) {
            return false;
        }
        if (entries == null || !entries.hasNext()) {
            if (!packs.hasNext()) {
                return false;
            }
            entries = packs.next().iterator();
        }

        objectId = entries.next().toObjectId();
        try {
            loader = fileRepo.open(objectId);
            if (loader.getType() != OBJ_BLOB) {
                return advanceNextPosition();
            }
        }
        catch (IOException e) {
            return advanceNextPosition();
        }

        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int field)
    {
        return Slices.utf8Slice(objectId.getName());
    }

    @Override
    public Object getObject(int field)
    {
        BlockBuilder blockBuilder = VarbinaryType.VARBINARY.createBlockBuilder(null, 15);
        byte[] bytes = loader.getBytes();
        Slice slice = Slices.wrappedBuffer(bytes, 0, bytes.length);
        VarbinaryType.VARBINARY.writeSlice(blockBuilder, slice);
        return blockBuilder.build();
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    @Override
    public void close()
    {
    }
}
