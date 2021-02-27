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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;
import io.trino.spi.type.VarcharType;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.RefDatabase;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

public class CommitsRecordCursor
        implements RecordCursor
{
    private final List<GitColumnHandle> columnHandles;
    private final Map<Integer, Function<RevCommit, Long>> longFieldGetters = new HashMap<>();
    private final Map<Integer, Function<RevCommit, String>> strFieldGetters = new HashMap<>();
    private final Map<Integer, Function<RevCommit, Object>> objFieldGetters = new HashMap<>();

    private final Iterator<RevCommit> commits;

    private RevCommit commit;

    public CommitsRecordCursor(List<GitColumnHandle> columnHandles, Git repo, Optional<List<String>> commitIds)
    {
        this.columnHandles = columnHandles;

        Map<String, Integer> nameToIndex = new HashMap<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            nameToIndex.put(columnHandles.get(i).getColumnName(), i);
        }

        if (nameToIndex.containsKey("commit_time")) {
            longFieldGetters.put(
                    nameToIndex.get("commit_time"),
                    c -> DateTimeEncoding.packDateTimeWithZone(c.getCommitTime() * 1000L, c.getCommitterIdent().getTimeZoneOffset()));
        }

        Map<String, Function<RevCommit, String>> getters = Map.of(
                "object_id", RevCommit::getName,
                "author_name", c -> c.getAuthorIdent().getName(),
                "author_email", c -> c.getAuthorIdent().getEmailAddress(),
                "committer_name", c -> c.getCommitterIdent().getName(),
                "committer_email", c -> c.getCommitterIdent().getEmailAddress(),
                "message", RevCommit::getFullMessage,
                "tree_id", c -> c.getTree().getName());

        for (Map.Entry<String, Function<RevCommit, String>> entry : getters.entrySet()) {
            String k = entry.getKey();
            if (nameToIndex.containsKey(k)) {
                strFieldGetters.put(nameToIndex.get(k), entry.getValue());
            }
        }

        if (nameToIndex.containsKey("parents")) {
            objFieldGetters.put(nameToIndex.get("parents"), CommitsRecordCursor::getParents);
        }

        RefDatabase refDb = repo.getRepository().getRefDatabase();
        RevWalk revWalk = new RevWalk(repo.getRepository());

        if (commitIds.isEmpty()) {
            try {
                Collection<Ref> allRefs = refDb.getRefs();
                for (Ref ref : allRefs) {
                    revWalk.markStart(revWalk.parseCommit(ref.getObjectId()));
                }
            }
            catch (IOException ignored) {
                // pass
            }
            commits = revWalk.iterator();
        }
        else {
            commits = commitIds.get().stream().map(id -> {
                try {
                    return revWalk.parseCommit(ObjectId.fromString(id));
                }
                catch (IOException ignored) {
                    // ignore invalid commits
                    return null;
                }
            }).filter(Objects::nonNull).iterator();
        }
    }

    public static Block getParents(RevCommit c)
    {
        Type elementType = VarcharType.VARCHAR;
        RevCommit[] parents = c.getParents();
        BlockBuilder builder = elementType.createBlockBuilder(null, parents.length);
        for (RevCommit p : parents) {
            TypeUtils.writeNativeValue(elementType, builder, p.getName());
        }
        return builder.build();
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
        if (commits == null || !commits.hasNext()) {
            return false;
        }

        commit = commits.next();

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
        checkArgument(longFieldGetters.containsKey(field), "Invalid field index");
        return longFieldGetters.get(field).apply(commit);
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
        return Slices.utf8Slice(strFieldGetters.get(field).apply(commit));
    }

    @Override
    public Object getObject(int field)
    {
        checkArgument(objFieldGetters.containsKey(field), "Invalid field index");
        return objFieldGetters.get(field).apply(commit);
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
