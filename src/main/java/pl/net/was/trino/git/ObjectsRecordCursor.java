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
package pl.net.was.trino.git;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.internal.storage.file.Pack;
import org.eclipse.jgit.internal.storage.file.PackIndex;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static org.eclipse.jgit.lib.Constants.OBJ_BLOB;

public class ObjectsRecordCursor
        implements RecordCursor
{
    private final List<GitColumnHandle> columnHandles;

    private final FileRepository fileRepo;
    private final Iterator<Pack> packs;
    private Iterator<PackIndex.MutableEntry> entries;
    private ObjectId objectId;
    private ObjectLoader loader;

    private final Map<Integer, Function<ObjectsRecordCursor, Slice>> strFieldGetters = new HashMap<>();

    public ObjectsRecordCursor(List<GitColumnHandle> columnHandles, Git repo)
    {
        this.columnHandles = columnHandles;

        Map<String, Integer> nameToIndex = new HashMap<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            nameToIndex.put(columnHandles.get(i).getColumnName(), i);
        }

        Map<String, Function<ObjectsRecordCursor, Slice>> getters = Map.of(
                "object_id", ObjectsRecordCursor::getObjectId,
                "contents", ObjectsRecordCursor::getContents);

        for (Map.Entry<String, Function<ObjectsRecordCursor, Slice>> entry : getters.entrySet()) {
            String k = entry.getKey();
            if (nameToIndex.containsKey(k)) {
                strFieldGetters.put(nameToIndex.get(k), entry.getValue());
            }
        }

        fileRepo = (FileRepository) repo.getRepository();
        Collection<Pack> packs = fileRepo.getObjectDatabase().getPacks();
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
        checkArgument(strFieldGetters.containsKey(field), "Invalid field index");
        return strFieldGetters.get(field).apply(this);
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
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

    private Slice getObjectId()
    {
        return Slices.utf8Slice(objectId.getName());
    }

    private Slice getContents()
    {
        byte[] bytes = loader.getBytes();
        return Slices.wrappedBuffer(bytes, 0, bytes.length);
    }
}
