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
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.Edit;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.EmptyTreeIterator;
import org.eclipse.jgit.util.io.NullOutputStream;

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

public class DiffStatsRecordCursor
        implements RecordCursor
{
    private final List<GitColumnHandle> columnHandles;
    private final int oldCommitIdField;
    private final Map<Integer, Function<DiffStatsRecordCursor, Integer>> intFieldGetters = new HashMap<>();
    private final Map<Integer, Function<DiffStatsRecordCursor, String>> strFieldGetters = new HashMap<>();

    private final RevWalk revWalk;
    private final ObjectReader reader;
    private final DiffFormatter formatter;
    private final Iterator<RevCommit> commits;
    private Iterator<DiffEntry> entries;

    private RevCommit currentCommit;
    private Optional<RevCommit> parentCommit;
    private DiffEntry entry;
    private EditList edits;

    private final Map<DiffEntry.ChangeType, String> changeTypeNames = Map.of(
            DiffEntry.ChangeType.ADD, "Add",
            DiffEntry.ChangeType.MODIFY, "Modify",
            DiffEntry.ChangeType.DELETE, "Delete",
            DiffEntry.ChangeType.RENAME, "Rename",
            DiffEntry.ChangeType.COPY, "Copy");

    public DiffStatsRecordCursor(List<GitColumnHandle> columnHandles, Git repo, Optional<List<String>> commitIds)
    {
        this.columnHandles = columnHandles;

        Map<String, Integer> nameToIndex = new HashMap<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            nameToIndex.put(columnHandles.get(i).columnName(), i);
        }
        oldCommitIdField = nameToIndex.getOrDefault("old_commit_id", -1);

        Map<String, Function<DiffStatsRecordCursor, Integer>> intGetters = Map.of(
                "similarity_score", c -> c.entry.getScore(),
                "added_lines", DiffStatsRecordCursor::getAddedLines,
                "deleted_lines", DiffStatsRecordCursor::getDeletedLines);

        for (Map.Entry<String, Function<DiffStatsRecordCursor, Integer>> entry : intGetters.entrySet()) {
            String k = entry.getKey();
            if (nameToIndex.containsKey(k)) {
                intFieldGetters.put(nameToIndex.get(k), entry.getValue());
            }
        }

        Map<String, Function<DiffStatsRecordCursor, String>> strGetters = Map.of(
                "commit_id", DiffStatsRecordCursor::getCommitId,
                "old_commit_id", DiffStatsRecordCursor::getOldCommitId,
                "object_id", DiffStatsRecordCursor::getObjectId,
                "path_name", DiffStatsRecordCursor::getPathName,
                "old_path_name", DiffStatsRecordCursor::getOldPathName,
                "change_type", c -> getChangeType(c.entry.getChangeType()));

        for (Map.Entry<String, Function<DiffStatsRecordCursor, String>> entry : strGetters.entrySet()) {
            String k = entry.getKey();
            if (nameToIndex.containsKey(k)) {
                strFieldGetters.put(nameToIndex.get(k), entry.getValue());
            }
        }

        revWalk = new RevWalk(repo.getRepository());
        commits = walkCommits(repo.getRepository(), revWalk, commitIds);

        reader = repo.getRepository().newObjectReader();
        formatter = new DiffFormatter(NullOutputStream.INSTANCE);
        formatter.setRepository(repo.getRepository());
        formatter.setDetectRenames(true);
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
            while (entries == null || !entries.hasNext()) {
                if (!commits.hasNext()) {
                    return false;
                }
                currentCommit = commits.next();
                // like `git log -p --first-parent`, merge commits are compared only with the first parent
                parentCommit = Optional.ofNullable(currentCommit.getParentCount() > 0 ? currentCommit.getParent(0) : null);
                entries = formatter.scan(getTree(parentCommit), getTree(Optional.of(currentCommit))).iterator();
            }

            entry = entries.next();
            edits = formatter.toFileHeader(entry).toEditList();
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to compare commit %s with its parent".formatted(currentCommit.getName()), e);
        }
        return true;
    }

    private AbstractTreeIterator getTree(Optional<RevCommit> commit)
            throws IOException
    {
        if (commit.isEmpty()) {
            return new EmptyTreeIterator();
        }
        // parents are not parsed when walking commits
        revWalk.parseHeaders(commit.orElseThrow());
        return new CanonicalTreeParser(null, reader, commit.orElseThrow().getTree());
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
        return intFieldGetters.get(field).apply(this);
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
        return field == oldCommitIdField && parentCommit.isEmpty();
    }

    @Override
    public void close() {}

    private String getChangeType(DiffEntry.ChangeType changeType)
    {
        if (!changeTypeNames.containsKey(changeType)) {
            // there are a few others, see FileMode javadoc for details
            throw new IllegalArgumentException("Unknown type of change encountered: " + changeType);
        }
        return changeTypeNames.get(changeType);
    }

    private String getCommitId()
    {
        return currentCommit.getName();
    }

    private String getOldCommitId()
    {
        return parentCommit.orElseThrow().getName();
    }

    private String getObjectId()
    {
        return entry.getNewId().name();
    }

    private String getPathName()
    {
        return entry.getNewPath();
    }

    private String getOldPathName()
    {
        return entry.getOldPath();
    }

    private int getAddedLines()
    {
        int result = 0;
        for (Edit edit : edits) {
            result += edit.getLengthB();
        }
        return result;
    }

    private int getDeletedLines()
    {
        int result = 0;
        for (Edit edit : edits) {
            result += edit.getLengthA();
        }
        return result;
    }
}
