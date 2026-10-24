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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.ObjectWalk;
import org.eclipse.jgit.revwalk.RevObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static org.eclipse.jgit.lib.Constants.OBJ_BLOB;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;

public class ObjectsRecordCursor
        implements RecordCursor
{
    private final List<GitColumnHandle> columnHandles;

    private final ObjectWalk walk;
    private boolean commitsWalked;
    private RevObject object;
    private Optional<Slice> contents;

    private final Map<Integer, Function<ObjectsRecordCursor, Slice>> strFieldGetters = new HashMap<>();

    public ObjectsRecordCursor(List<GitColumnHandle> columnHandles, Git repo)
    {
        this.columnHandles = columnHandles;

        Map<String, Integer> nameToIndex = new HashMap<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            nameToIndex.put(columnHandles.get(i).columnName(), i);
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

        walk = new ObjectWalk(repo.getRepository());
        try {
            for (Ref ref : repo.getRepository().getRefDatabase().getRefs()) {
                if (ref.getObjectId() != null) {
                    // refs can point to annotated tags, and these, like any ref, to trees or blobs
                    walk.markStart(walk.peel(walk.parseAny(ref.getObjectId())));
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to read refs", e);
        }
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
        return columnHandles.get(field).columnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            if (!commitsWalked) {
                // an object walk returns trees and blobs only after all commits
                while (walk.next() != null) {
                    // skip commits
                }
                commitsWalked = true;
            }
            do {
                object = walk.nextObject();
                if (object == null) {
                    return false;
                }
            }
            while (object.getType() != OBJ_BLOB);
            contents = null;
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to read objects", e);
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
        return strFieldGetters.get(field).apply(this) == null;
    }

    @Override
    public void close()
    {
        walk.close();
    }

    private Slice getObjectId()
    {
        return Slices.utf8Slice(object.getName());
    }

    private Slice getContents()
    {
        if (contents != null) {
            return contents.orElse(null);
        }
        try {
            ObjectLoader loader = walk.getObjectReader().open(object, OBJ_BLOB);
            // blobs larger than core.streamFileThreshold can't be loaded into memory at once
            contents = loader.isLarge() ? Optional.empty() : Optional.of(Slices.wrappedBuffer(loader.getCachedBytes()));
            return contents.orElse(null);
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to read object " + object.getName(), e);
        }
    }
}
