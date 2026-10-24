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
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static pl.net.was.trino.git.CommitWalks.walkCommits;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;

public class TreesRecordCursor
        implements RecordCursor
{
    private final List<GitColumnHandle> columnHandles;
    private final Map<Integer, Function<TreeWalk, Integer>> intFieldGetters = new HashMap<>();
    private final Map<Integer, Function<TreesRecordCursor, String>> strFieldGetters = new HashMap<>();

    private final Iterator<RevCommit> commits;
    private final TreeWalk treeWalk;
    private RevCommit commit;

    private final Map<FileMode, String> fileModeNames = Map.of(
            FileMode.REGULAR_FILE, "file",
            FileMode.EXECUTABLE_FILE, "executable",
            FileMode.SYMLINK, "symlink",
            FileMode.GITLINK, "submodule",
            FileMode.TREE, "directory");

    public TreesRecordCursor(List<GitColumnHandle> columnHandles, Git repo, Optional<List<String>> commitIds)
    {
        this.columnHandles = columnHandles;

        Map<String, Integer> nameToIndex = new HashMap<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            nameToIndex.put(columnHandles.get(i).columnName(), i);
        }

        if (nameToIndex.containsKey("depth")) {
            intFieldGetters.put(nameToIndex.get("depth"), TreeWalk::getDepth);
        }

        Map<String, Function<TreesRecordCursor, String>> getters = Map.of(
                "commit_id", TreesRecordCursor::getCommitId,
                "file_mode", TreesRecordCursor::getFileMode,
                "mode", TreesRecordCursor::getMode,
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

        RevWalk revWalk = new RevWalk(repo.getRepository());
        commits = walkCommits(repo.getRepository(), revWalk, commitIds);
        // subtrees are entered explicitly, to return directories before their contents
        treeWalk = new TreeWalk(repo.getRepository());
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
        if (commits == null) {
            return false;
        }

        try {
            if (commit != null && treeWalk.isSubtree()) {
                treeWalk.enterSubtree();
            }
            // commits can have empty trees, like an orphan branch with an empty first commit
            while (commit == null || !treeWalk.next()) {
                if (!commits.hasNext()) {
                    return false;
                }
                commit = commits.next();
                treeWalk.reset(commit.getTree());
            }
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to read the tree of commit " + commit.getName(), e);
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
    public void close() {}

    private String getCommitId()
    {
        return commit.getName();
    }

    private String getFileMode()
    {
        // there are also deprecated modes, used only by very old versions of git
        return fileModeNames.getOrDefault(treeWalk.getFileMode(), "unknown");
    }

    private String getMode()
    {
        // like git ls-tree
        return "%06o".formatted(treeWalk.getRawMode(0));
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
