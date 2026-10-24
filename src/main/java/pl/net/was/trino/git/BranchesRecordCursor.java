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
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.eclipse.jgit.lib.Constants.HEAD;
import static org.eclipse.jgit.lib.Constants.R_HEADS;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;

public class BranchesRecordCursor
        implements RecordCursor
{
    private final List<GitColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final RevWalk walk;
    private final Optional<RevCommit> head;

    private Iterator<Ref> branches;
    private Ref branch;

    private List<String> fields;

    public BranchesRecordCursor(List<GitColumnHandle> columnHandles, Git repo, List<Ref> refs)
    {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            fieldToColumnIndex[i] = columnHandles.get(i).ordinalPosition();
        }

        branches = refs.stream()
                .filter(ref -> ref.getName().startsWith(R_HEADS))
                .sorted(Comparator.comparing(Ref::getName))
                .iterator();

        walk = new RevWalk(repo.getRepository());
        try {
            head = getCommit(refs.stream().filter(ref -> ref.getName().equals(HEAD)).findFirst().orElse(null));
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to read HEAD", e);
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
        if (!branches.hasNext()) {
            return false;
        }
        branch = branches.next();

        fields = List.of(
                branch.getObjectId().getName(),
                branch.getName());

        return true;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field)
    {
        try {
            return walk.isMergedInto(getCommit(branch).orElseThrow(), head.orElseThrow());
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to check if %s is merged".formatted(branch.getName()), e);
        }
    }

    private Optional<RevCommit> getCommit(Ref ref)
            throws IOException
    {
        // HEAD of an empty repository doesn't point to any object, and branches can point to any object
        if (ref == null || ref.getObjectId() == null) {
            return Optional.empty();
        }
        if (walk.peel(walk.parseAny(ref.getObjectId())) instanceof RevCommit commit) {
            return Optional.of(commit);
        }
        return Optional.empty();
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
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        if (!columnHandles.get(field).columnName().equals("is_merged")) {
            return false;
        }
        try {
            return head.isEmpty() || getCommit(branch).isEmpty();
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to read branch " + branch.getName(), e);
        }
    }

    @Override
    public void close()
    {
        walk.close();
    }
}
