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
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import io.trino.spi.TrinoException;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.Edit;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.lib.AbbreviatedObjectId;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.patch.FileHeader;
import org.eclipse.jgit.patch.FileHeader.PatchType;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.EmptyTreeIterator;
import org.eclipse.jgit.util.io.NullOutputStream;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static pl.net.was.trino.git.ColumnValues.toSlice;
import static pl.net.was.trino.git.CommitWalks.walkCommits;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;
import static pl.net.was.trino.git.TableColumn.column;

public class DiffStatsTable
        implements GitTableReader<DiffStatsTable.Change>
{
    private static final List<TableColumn<Change>> COLUMNS = List.of(
            column("commit_id", VARCHAR, "Commit id", change -> utf8Slice(change.commitId())),
            column("old_commit_id", VARCHAR, "Id of the first parent commit, NULL for a root commit", change -> toSlice(change.oldCommitId())),
            column("object_id", VARCHAR, "Id of the new file contents, NULL for a deleted file", change -> toSlice(getId(change.entry().getNewId()))),
            column("old_object_id", VARCHAR, "Id of the old file contents, NULL for an added file", change -> toSlice(getId(change.entry().getOldId()))),
            column("path_name", VARCHAR, "New file path, NULL for a deleted file", change -> toSlice(getPath(change.entry().getNewPath()))),
            column("old_path_name", VARCHAR, "Old file path, NULL for an added file", change -> toSlice(getPath(change.entry().getOldPath()))),
            column("change_type", VARCHAR, "One of added, modified, deleted, renamed, copied", change -> utf8Slice(getChangeType(change.entry().getChangeType()))),
            column("similarity_score", INTEGER, "Similarity of the old and new file for renames and copies, 0 to 100", change -> (long) change.entry().getScore()),
            column("added_lines", INTEGER, "Number of added lines, NULL for binary files", change -> change.edits().get().map(edits -> (long) edits.stream().mapToInt(Edit::getLengthB).sum()).orElse(null)),
            column("deleted_lines", INTEGER, "Number of deleted lines, NULL for binary files", change -> change.edits().get().map(edits -> (long) edits.stream().mapToInt(Edit::getLengthA).sum()).orElse(null)));

    public record Change(String commitId, Optional<String> oldCommitId, DiffEntry entry, Supplier<Optional<EditList>> edits) {}

    @Override
    public List<TableColumn<Change>> getColumns()
    {
        return COLUMNS;
    }

    @Override
    public RowIterator<Change> read(Git repo, List<Ref> refs, Optional<List<String>> ids, Optional<List<String>> paths)
    {
        return new DiffsIterator(repo.getRepository(), refs, ids, paths);
    }

    private static String getChangeType(DiffEntry.ChangeType changeType)
    {
        return switch (changeType) {
            case ADD -> "added";
            case MODIFY -> "modified";
            case DELETE -> "deleted";
            case RENAME -> "renamed";
            case COPY -> "copied";
        };
    }

    private static Optional<String> getId(AbbreviatedObjectId id)
    {
        // added files have no old id, and deleted files have no new id
        return id.toObjectId().equals(ObjectId.zeroId()) ? Optional.empty() : Optional.of(id.name());
    }

    private static Optional<String> getPath(String path)
    {
        return path.equals(DiffEntry.DEV_NULL) ? Optional.empty() : Optional.of(path);
    }

    private static final class DiffsIterator
            extends AbstractIterator<Change>
            implements RowIterator<Change>
    {
        private final Optional<Set<String>> paths;
        private final RevWalk revWalk;
        private final ObjectReader reader;
        private final DiffFormatter formatter;
        private final Iterator<RevCommit> commits;
        private Iterator<DiffEntry> entries;
        private RevCommit commit;
        private Optional<RevCommit> parent;

        private DiffsIterator(Repository repository, List<Ref> refs, Optional<List<String>> commitIds, Optional<List<String>> paths)
        {
            this.paths = paths.map(ImmutableSet::copyOf);
            revWalk = new RevWalk(repository);
            commits = walkCommits(revWalk, refs, commitIds);
            reader = repository.newObjectReader();
            formatter = new DiffFormatter(NullOutputStream.INSTANCE);
            formatter.setRepository(repository);
            formatter.setDetectRenames(true);
        }

        @Override
        protected Change computeNext()
        {
            try {
                while (entries == null || !entries.hasNext()) {
                    if (!commits.hasNext()) {
                        return endOfData();
                    }
                    commit = commits.next();
                    // like `git log -p --first-parent`, merge commits are compared only with the first parent
                    parent = Optional.ofNullable(commit.getParentCount() > 0 ? commit.getParent(0) : null);
                    // skip other files after detecting renames, which a path filter in the formatter would break
                    entries = Iterators.filter(formatter.scan(getTree(parent), getTree(Optional.of(commit))).iterator(), this::isSelected);
                }
            }
            catch (IOException e) {
                throw new TrinoException(GIT_READ_ERROR, "Failed to compare commit %s with its parent".formatted(commit.getName()), e);
            }
            DiffEntry entry = entries.next();
            String commitId = commit.getName();
            return new Change(commitId, parent.map(RevCommit::getName), entry, Suppliers.memoize(() -> readEdits(entry, commitId)));
        }

        private Optional<EditList> readEdits(DiffEntry entry, String commitId)
        {
            try {
                FileHeader header = formatter.toFileHeader(entry);
                // line counts are unknown for binary files
                return header.getPatchType() == PatchType.UNIFIED ? Optional.of(header.toEditList()) : Optional.empty();
            }
            catch (IOException e) {
                throw new TrinoException(GIT_READ_ERROR, "Failed to compare commit %s with its parent".formatted(commitId), e);
            }
        }

        private boolean isSelected(DiffEntry entry)
        {
            return paths.map(selected -> selected.contains(entry.getNewPath()) || selected.contains(entry.getOldPath())).orElse(true);
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
        public void close()
        {
            formatter.close();
            reader.close();
            revWalk.close();
        }
    }
}
