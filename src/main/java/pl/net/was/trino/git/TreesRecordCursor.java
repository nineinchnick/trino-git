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
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.RefDatabase;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;

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

public class TreesRecordCursor
        implements RecordCursor
{
    private final List<GitColumnHandle> columnHandles;
    private final Map<Integer, Function<TreeWalk, Integer>> intFieldGetters = new HashMap<>();
    private final Map<Integer, Function<TreesRecordCursor, String>> strFieldGetters = new HashMap<>();

    private final Git repo;
    private final Iterator<RevCommit> commits;
    private TreeWalk treeWalk;
    private RevCommit commit;

    private final Map<FileMode, String> fileModeNames = Map.of(
            FileMode.EXECUTABLE_FILE, "Executable File",
            FileMode.REGULAR_FILE, "Normal File",
            FileMode.TREE, "Directory",
            FileMode.SYMLINK, "Symlink",
            FileMode.GITLINK, "Gitlink");

    public TreesRecordCursor(List<GitColumnHandle> columnHandles, Git repo, Optional<List<String>> commitIds)
    {
        this.repo = repo;
        this.columnHandles = columnHandles;

        Map<String, Integer> nameToIndex = new HashMap<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            nameToIndex.put(columnHandles.get(i).getColumnName(), i);
        }

        if (nameToIndex.containsKey("depth")) {
            intFieldGetters.put(nameToIndex.get("depth"), TreeWalk::getDepth);
        }

        Map<String, Function<TreesRecordCursor, String>> getters = Map.of(
                "commit_id", TreesRecordCursor::getCommitId,
                "object_type", TreesRecordCursor::getObjectType,
                "object_id", TreesRecordCursor::getObjectId,
                "file_name", TreesRecordCursor::getFileName,
                "path_name", TreesRecordCursor::getPathName,
                "attributes", TreesRecordCursor::getAttributes);

        for (Map.Entry<String, Function<TreesRecordCursor, String>> entry : getters.entrySet()) {
            String k = entry.getKey();
            if (nameToIndex.containsKey(k)) {
                strFieldGetters.put(nameToIndex.get(k), entry.getValue());
            }
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

    private String getFileMode(FileMode fileMode)
    {
        if (!fileModeNames.containsKey(fileMode)) {
            // there are a few others, see FileMode javadoc for details
            throw new IllegalArgumentException("Unknown type of file encountered: " + fileMode);
        }
        return fileModeNames.get(fileMode);
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
        if (commits == null) {
            return false;
        }

        try {
            if (treeWalk == null || !treeWalk.next()) {
                if (!commits.hasNext()) {
                    return false;
                }
                commit = commits.next();
                treeWalk = new TreeWalk(repo.getRepository());
                treeWalk.addTree(commit.getTree());
                treeWalk.setRecursive(true);
                if (!treeWalk.next()) {
                    return false;
                }
            }
        }
        catch (IOException ignores) {
            // pass
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
        checkArgument(intFieldGetters.containsKey(field), "Invalid field index");
        return intFieldGetters.get(field).apply(treeWalk);
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
        return Slices.utf8Slice(strFieldGetters.get(field).apply(this));
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

    private String getCommitId()
    {
        return commit.getName();
    }

    private String getObjectType()
    {
        return getFileMode(treeWalk.getFileMode());
    }

    private String getObjectId()
    {
        return treeWalk.getObjectId(0).getName();
    }

    private String getFileName()
    {
        return treeWalk.getNameString();
    }

    private String getPathName()
    {
        return treeWalk.getPathString();
    }

    private String getAttributes()
    {
        return treeWalk.getAttributes().toString();
    }
}
