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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Ref;
import pl.net.was.trino.git.Identities.Group;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;

public class IdentsRecordCursor
        implements RecordCursor
{
    private final List<GitColumnHandle> columnHandles;
    private final Iterator<Group> groups;
    private Group group;

    public IdentsRecordCursor(List<GitColumnHandle> columnHandles, Git repo, List<Ref> refs)
    {
        this.columnHandles = columnHandles;
        Mailmap mailmap;
        try {
            mailmap = Mailmap.read(repo.getRepository(), refs);
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to read .mailmap", e);
        }
        groups = Identities.group(repo.getRepository(), refs, mailmap).iterator();
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
        if (!groups.hasNext()) {
            return false;
        }
        group = groups.next();
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
        checkState(group != null, "Cursor has not been advanced yet");
        return switch (columnHandles.get(field).columnName()) {
            case "email" -> Slices.utf8Slice(group.primary().email());
            case "name" -> Slices.utf8Slice(group.primary().name());
            default -> throw new IllegalArgumentException("Invalid field index: " + field);
        };
    }

    @Override
    public Object getObject(int field)
    {
        checkState(group != null, "Cursor has not been advanced yet");
        return switch (columnHandles.get(field).columnName()) {
            case "extra_emails" -> toBlock(group.extraEmails());
            case "extra_names" -> toBlock(group.extraNames());
            default -> throw new IllegalArgumentException("Invalid field index: " + field);
        };
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    @Override
    public void close() {}

    private static Block toBlock(List<String> values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, values.size());
        values.forEach(value -> VARCHAR.writeString(builder, value));
        return builder.build();
    }
}
