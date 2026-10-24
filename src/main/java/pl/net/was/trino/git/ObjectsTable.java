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

import com.google.common.base.Suppliers;
import com.google.common.collect.AbstractIterator;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.ObjectWalk;
import org.eclipse.jgit.revwalk.RevObject;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.eclipse.jgit.lib.Constants.OBJ_BLOB;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;
import static pl.net.was.trino.git.TableColumn.column;

public class ObjectsTable
        implements GitTableReader<ObjectsTable.Blob>
{
    private static final List<TableColumn<Blob>> COLUMNS = List.of(
            column("object_id", VARCHAR, "Blob id", blob -> utf8Slice(blob.objectId())),
            column("contents", VARBINARY, "File contents, NULL for blobs too large to load, over 50 MB by default", blob -> blob.contents().get().map(Slices::wrappedBuffer).orElse(null)));

    public record Blob(String objectId, Supplier<Optional<byte[]>> contents) {}

    @Override
    public List<TableColumn<Blob>> getColumns()
    {
        return COLUMNS;
    }

    @Override
    public RowIterator<Blob> read(Git repo, List<Ref> refs, Optional<List<String>> ids, Optional<List<String>> paths)
    {
        return new ObjectsIterator(repo.getRepository(), refs, ids);
    }

    private static final class ObjectsIterator
            extends AbstractIterator<Blob>
            implements RowIterator<Blob>
    {
        private final ObjectWalk walk;
        private final Optional<Iterator<String>> objectIds;
        private boolean commitsWalked;

        private ObjectsIterator(Repository repository, List<Ref> refs, Optional<List<String>> objectIds)
        {
            walk = new ObjectWalk(repository);
            this.objectIds = objectIds.map(List::iterator);
            if (objectIds.isPresent()) {
                return;
            }
            try {
                for (Ref ref : refs) {
                    if (ref.getObjectId() != null) {
                        // refs can point to annotated tags, and these, like any ref, to trees or blobs
                        walk.markStart(walk.peel(walk.parseAny(ref.getObjectId())));
                    }
                }
            }
            catch (IOException e) {
                walk.close();
                throw new TrinoException(GIT_READ_ERROR, "Failed to read refs", e);
            }
        }

        @Override
        protected Blob computeNext()
        {
            Optional<RevObject> object = objectIds.isPresent() ? nextSelectedObject(objectIds.orElseThrow()) : nextObject();
            if (object.isEmpty()) {
                return endOfData();
            }
            RevObject blob = object.orElseThrow();
            return new Blob(blob.getName(), Suppliers.memoize(() -> readContents(blob)));
        }

        private Optional<RevObject> nextObject()
        {
            try {
                if (!commitsWalked) {
                    // an object walk returns trees and blobs only after all commits
                    while (walk.next() != null) {
                        // skip commits
                    }
                    commitsWalked = true;
                }
                RevObject object;
                do {
                    object = walk.nextObject();
                    if (object == null) {
                        return Optional.empty();
                    }
                }
                while (object.getType() != OBJ_BLOB);
                return Optional.of(object);
            }
            catch (IOException e) {
                throw new TrinoException(GIT_READ_ERROR, "Failed to read objects", e);
            }
        }

        private Optional<RevObject> nextSelectedObject(Iterator<String> ids)
        {
            while (ids.hasNext()) {
                try {
                    RevObject selected = walk.parseAny(ObjectId.fromString(ids.next()));
                    if (selected.getType() == OBJ_BLOB) {
                        return Optional.of(selected);
                    }
                }
                catch (MissingObjectException _) {
                    // ids from filters don't have to exist, they just don't match any rows
                }
                catch (IOException e) {
                    throw new TrinoException(GIT_READ_ERROR, "Failed to read objects", e);
                }
            }
            return Optional.empty();
        }

        private Optional<byte[]> readContents(RevObject blob)
        {
            try {
                ObjectLoader loader = walk.getObjectReader().open(blob, OBJ_BLOB);
                // blobs larger than core.streamFileThreshold can't be loaded into memory at once
                return loader.isLarge() ? Optional.empty() : Optional.of(loader.getCachedBytes());
            }
            catch (IOException e) {
                throw new TrinoException(GIT_READ_ERROR, "Failed to read object " + blob.getName(), e);
            }
        }

        @Override
        public void close()
        {
            walk.close();
        }
    }
}
