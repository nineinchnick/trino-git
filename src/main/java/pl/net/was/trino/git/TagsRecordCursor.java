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
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevTag;
import org.eclipse.jgit.revwalk.RevWalk;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static java.util.Arrays.asList;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;

public class TagsRecordCursor
        implements RecordCursor
{
    private final List<GitColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final boolean parseTag;
    private final RevWalk walk;
    private Iterator<Ref> tags;

    private List<Object> fields;

    public TagsRecordCursor(List<GitColumnHandle> columnHandles, Git repo)
    {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            fieldToColumnIndex[i] = columnHandles.get(i).ordinalPosition();
        }

        parseTag = columnHandles.stream()
                .anyMatch(column -> column.columnName().equals("commit_id") || column.columnName().equals("tag_time"));
        walk = parseTag ? new RevWalk(repo.getRepository()) : null;
        try {
            tags = repo.tagList().call().iterator();
        }
        catch (GitAPIException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to list tags", e);
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
        if (walk != null) {
            walk.reset();
        }
        if (!tags.hasNext()) {
            return false;
        }
        Ref tag = tags.next();

        String commitId = null;
        Long tagTime = null;
        if (parseTag) {
            try {
                RevObject object = walk.parseAny(tag.getObjectId());
                // tags can point to other tags, trees or blobs
                if (walk.peel(object) instanceof RevCommit commit) {
                    commitId = commit.getName();
                }
                // tags made before git added tagger information don't have it
                if (object instanceof RevTag revTag && revTag.getTaggerIdent() != null) {
                    PersonIdent tagger = revTag.getTaggerIdent();
                    tagTime = packDateTimeWithZone(tagger.getWhenAsInstant().toEpochMilli(), getTimeZoneKey(tagger.getZoneId().getId()));
                }
            }
            catch (IOException e) {
                throw new TrinoException(GIT_READ_ERROR, "Failed to read tag " + tag.getName(), e);
            }
        }

        fields = asList(
                tag.getObjectId().getName(),
                commitId,
                tag.getName(),
                tagTime);

        return true;
    }

    private Object getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int field)
    {
        return (Long) getFieldValue(field);
    }

    @Override
    public double getDouble(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int field)
    {
        return Slices.utf8Slice((String) getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        return getFieldValue(field) == null;
    }

    @Override
    public void close()
    {
        if (walk != null) {
            walk.close();
        }
    }
}
