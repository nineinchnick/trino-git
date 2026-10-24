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

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Ref;
import pl.net.was.trino.git.Identities.Group;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static pl.net.was.trino.git.ColumnValues.toVarcharArray;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;
import static pl.net.was.trino.git.TableColumn.column;

public class IdentsTable
        implements GitTableReader<Group>
{
    private static final List<TableColumn<Group>> COLUMNS = List.of(
            column("email", VARCHAR, "Main email", group -> utf8Slice(group.primary().email())),
            column("name", VARCHAR, "Main name", group -> utf8Slice(group.primary().name())),
            column("extra_emails", new ArrayType(VARCHAR), "Other emails used by the same person", group -> toVarcharArray(group.extraEmails())),
            column("extra_names", new ArrayType(VARCHAR), "Other names used by the same person", group -> toVarcharArray(group.extraNames())));

    @Override
    public List<TableColumn<Group>> getColumns()
    {
        return COLUMNS;
    }

    @Override
    public RowIterator<Group> read(Git repo, List<Ref> refs, Optional<List<String>> ids, Optional<List<String>> paths)
    {
        Mailmap mailmap;
        try {
            mailmap = Mailmap.read(repo.getRepository(), refs);
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to read .mailmap", e);
        }
        return RowIterator.of(Identities.group(repo.getRepository(), refs, mailmap).iterator(), group -> group, () -> {});
    }
}
