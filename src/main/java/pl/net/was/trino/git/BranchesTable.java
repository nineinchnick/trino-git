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
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.eclipse.jgit.lib.Constants.HEAD;
import static org.eclipse.jgit.lib.Constants.R_HEADS;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;
import static pl.net.was.trino.git.TableColumn.column;

public class BranchesTable
        implements GitTableReader<BranchesTable.Branch>
{
    private static final List<TableColumn<Branch>> COLUMNS = List.of(
            column("object_id", VARCHAR, "Id of the commit the branch points to", branch -> utf8Slice(branch.ref().getObjectId().getName())),
            column("name", VARCHAR, "Full ref name, like refs/heads/main", branch -> utf8Slice(branch.ref().getName())),
            column("is_merged", BOOLEAN, "Whether the branch is merged into the HEAD commit", branch -> branch.isMerged().get().orElse(null)));

    public record Branch(Ref ref, Supplier<Optional<Boolean>> isMerged) {}

    @Override
    public List<TableColumn<Branch>> getColumns()
    {
        return COLUMNS;
    }

    @Override
    public RowIterator<Branch> read(Git repo, List<Ref> refs, Optional<List<String>> ids, Optional<List<String>> paths)
    {
        RevWalk walk = new RevWalk(repo.getRepository());
        Optional<RevCommit> head;
        try {
            head = getCommit(walk, refs.stream().filter(ref -> ref.getName().equals(HEAD)).findFirst());
        }
        catch (IOException e) {
            walk.close();
            throw new TrinoException(GIT_READ_ERROR, "Failed to read HEAD", e);
        }
        Iterator<Ref> branches = refs.stream()
                .filter(ref -> ref.getName().startsWith(R_HEADS))
                .sorted(Comparator.comparing(Ref::getName))
                .iterator();
        return RowIterator.of(branches, branch -> new Branch(branch, Suppliers.memoize(() -> isMerged(walk, branch, head))), walk::close);
    }

    private static Optional<Boolean> isMerged(RevWalk walk, Ref branch, Optional<RevCommit> head)
    {
        try {
            Optional<RevCommit> commit = getCommit(walk, Optional.of(branch));
            if (head.isEmpty() || commit.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(walk.isMergedInto(commit.orElseThrow(), head.orElseThrow()));
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to check if %s is merged".formatted(branch.getName()), e);
        }
    }

    private static Optional<RevCommit> getCommit(RevWalk walk, Optional<Ref> ref)
            throws IOException
    {
        // HEAD of an empty repository doesn't point to any object, and branches can point to any object
        if (ref.isEmpty() || ref.orElseThrow().getObjectId() == null) {
            return Optional.empty();
        }
        if (walk.peel(walk.parseAny(ref.orElseThrow().getObjectId())) instanceof RevCommit commit) {
            return Optional.of(commit);
        }
        return Optional.empty();
    }
}
