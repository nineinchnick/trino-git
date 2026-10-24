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

import io.trino.spi.type.ArrayType;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.stream;
import static pl.net.was.trino.git.ColumnValues.toTimestamp;
import static pl.net.was.trino.git.ColumnValues.toVarcharArray;
import static pl.net.was.trino.git.CommitWalks.walkCommits;
import static pl.net.was.trino.git.TableColumn.column;

public class CommitsTable
        implements GitTableReader<RevCommit>
{
    private static final List<TableColumn<RevCommit>> COLUMNS = List.of(
            column("object_id", VARCHAR, "Commit id", commit -> utf8Slice(commit.getName())),
            column("author_name", VARCHAR, "Author name", commit -> utf8Slice(commit.getAuthorIdent().getName())),
            column("author_email", VARCHAR, "Author email", commit -> utf8Slice(commit.getAuthorIdent().getEmailAddress())),
            column("committer_name", VARCHAR, "Committer name", commit -> utf8Slice(commit.getCommitterIdent().getName())),
            column("committer_email", VARCHAR, "Committer email", commit -> utf8Slice(commit.getCommitterIdent().getEmailAddress())),
            column("message", VARCHAR, "Full commit message", commit -> utf8Slice(commit.getFullMessage())),
            column("parents", new ArrayType(VARCHAR), "Ids of the parent commits", commit -> toVarcharArray(stream(commit.getParents()).map(RevCommit::getName).collect(toImmutableList()))),
            column("tree_id", VARCHAR, "Id of the root tree", commit -> utf8Slice(commit.getTree().getName())),
            column("commit_time", TIMESTAMP_TZ_SECONDS, "Time when the commit was made, in the committer's time zone", commit -> toTimestamp(commit.getCommitterIdent())),
            column("author_time", TIMESTAMP_TZ_SECONDS, "Time when the change was originally made, in the author's time zone, which differs from commit_time after a rebase or a cherry-pick", commit -> toTimestamp(commit.getAuthorIdent())));

    @Override
    public List<TableColumn<RevCommit>> getColumns()
    {
        return COLUMNS;
    }

    @Override
    public RowIterator<RevCommit> read(Git repo, List<Ref> refs, Optional<List<String>> ids, Optional<List<String>> paths)
    {
        RevWalk revWalk = new RevWalk(repo.getRepository());
        return RowIterator.of(walkCommits(revWalk, refs, ids), commit -> commit, revWalk::close);
    }
}
