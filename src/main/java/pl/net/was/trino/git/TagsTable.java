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
import io.trino.spi.TrinoException;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevTag;
import org.eclipse.jgit.revwalk.RevWalk;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.eclipse.jgit.lib.Constants.R_TAGS;
import static pl.net.was.trino.git.ColumnValues.toSlice;
import static pl.net.was.trino.git.ColumnValues.toTimestamp;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;
import static pl.net.was.trino.git.TableColumn.column;

public class TagsTable
        implements GitTableReader<TagsTable.Tag>
{
    private static final List<TableColumn<Tag>> COLUMNS = List.of(
            column("object_id", VARCHAR, "Id of the object the tag points to: a tag object or a commit", tag -> utf8Slice(tag.ref().getObjectId().getName())),
            column("commit_id", VARCHAR, "Id of the tagged commit, NULL if the tag points to a tree or a blob", tag -> toSlice(tag.target().get().commitId())),
            column("name", VARCHAR, "Full ref name, like refs/tags/v1.0", tag -> utf8Slice(tag.ref().getName())),
            column("tag_time", TIMESTAMP_TZ_SECONDS, "Time of an annotated tag, NULL for lightweight tags and tags without one", tag -> tag.target().get().tagger().map(ColumnValues::toTimestamp).orElse(null)));

    public record Tag(Ref ref, Supplier<Target> target) {}

    public record Target(Optional<String> commitId, Optional<PersonIdent> tagger) {}

    @Override
    public List<TableColumn<Tag>> getColumns()
    {
        return COLUMNS;
    }

    @Override
    public RowIterator<Tag> read(Git repo, List<Ref> refs, Optional<List<String>> ids, Optional<List<String>> paths)
    {
        RevWalk walk = new RevWalk(repo.getRepository());
        Iterator<Ref> tags = refs.stream()
                .filter(ref -> ref.getName().startsWith(R_TAGS))
                .sorted(Comparator.comparing(Ref::getName))
                .iterator();
        return RowIterator.of(tags, tag -> new Tag(tag, Suppliers.memoize(() -> readTarget(walk, tag))), walk::close);
    }

    private static Target readTarget(RevWalk walk, Ref tag)
    {
        try {
            RevObject object = walk.parseAny(tag.getObjectId());
            // tags can point to other tags, trees or blobs
            Optional<String> commitId = walk.peel(object) instanceof RevCommit commit ? Optional.of(commit.getName()) : Optional.empty();
            // tags made before git added tagger information don't have it
            Optional<PersonIdent> tagger = object instanceof RevTag revTag ? Optional.ofNullable(revTag.getTaggerIdent()) : Optional.empty();
            return new Target(commitId, tagger);
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to read tag " + tag.getName(), e);
        }
    }
}
