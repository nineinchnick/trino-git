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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import io.trino.spi.TrinoException;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.attributes.Attribute;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilterGroup;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static pl.net.was.trino.git.ColumnValues.toVarcharMap;
import static pl.net.was.trino.git.CommitWalks.walkCommits;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;
import static pl.net.was.trino.git.TableColumn.column;

public class TreesTable
        implements GitTableReader<TreesTable.TreeEntry>
{
    private static final MapType ATTRIBUTES_TYPE = new MapType(VARCHAR, VARCHAR, new TypeOperators());
    private static final Map<FileMode, String> FILE_MODES = Map.of(
            FileMode.REGULAR_FILE, "file",
            FileMode.EXECUTABLE_FILE, "executable",
            FileMode.SYMLINK, "symlink",
            FileMode.GITLINK, "submodule",
            FileMode.TREE, "directory");
    private static final List<TableColumn<TreeEntry>> COLUMNS = List.of(
            column("commit_id", VARCHAR, "Commit id", entry -> utf8Slice(entry.commitId())),
            // there are also deprecated modes, used only by very old versions of git
            column("file_mode", VARCHAR, "One of file, executable, symlink, submodule, directory", entry -> utf8Slice(FILE_MODES.getOrDefault(entry.fileMode(), "unknown"))),
            // like git ls-tree
            column("mode", VARCHAR, "Raw file mode in octal, like 100644, the same as in git ls-tree", entry -> utf8Slice("%06o".formatted(entry.mode()))),
            column("object_id", VARCHAR, "Id of the file contents, the directory tree, or the submodule commit", entry -> utf8Slice(entry.objectId())),
            column("file_name", VARCHAR, "File name", entry -> utf8Slice(entry.fileName())),
            column("path_name", VARCHAR, "Full file path", entry -> utf8Slice(entry.pathName())),
            column("attributes", ATTRIBUTES_TYPE, "Git attributes of the file from .gitattributes, true or false for attributes that are set or unset", entry -> toVarcharMap(ATTRIBUTES_TYPE, entry.attributes().get())),
            column("depth", INTEGER, "Number of parent directories of the file", entry -> (long) entry.depth()));

    public record TreeEntry(String commitId, FileMode fileMode, int mode, String objectId, String fileName, String pathName, int depth, Supplier<Map<String, String>> attributes) {}

    @Override
    public List<TableColumn<TreeEntry>> getColumns()
    {
        return COLUMNS;
    }

    @Override
    public RowIterator<TreeEntry> read(Git repo, List<Ref> refs, Optional<List<String>> ids, Optional<List<String>> paths)
    {
        return new TreesIterator(repo.getRepository(), refs, ids, paths);
    }

    private static final class TreesIterator
            extends AbstractIterator<TreeEntry>
            implements RowIterator<TreeEntry>
    {
        private final RevWalk revWalk;
        private final TreeWalk treeWalk;
        private Iterator<RevCommit> commits;
        private RevCommit commit;

        private TreesIterator(Repository repository, List<Ref> refs, Optional<List<String>> commitIds, Optional<List<String>> paths)
        {
            revWalk = new RevWalk(repository);
            commits = walkCommits(revWalk, refs, commitIds);
            // subtrees are entered explicitly, to return directories before their contents
            treeWalk = new TreeWalk(repository);
            // directories containing these paths are returned too, so the filter is not enforced
            paths.map(TreesIterator::getValidPaths).ifPresent(validPaths -> {
                if (validPaths.isEmpty()) {
                    commits = Iterators.forArray();
                    return;
                }
                treeWalk.setFilter(PathFilterGroup.createFromStrings(validPaths));
            });
        }

        @Override
        protected TreeEntry computeNext()
        {
            try {
                if (commit != null && treeWalk.isSubtree()) {
                    treeWalk.enterSubtree();
                }
                // commits can have empty trees, like an orphan branch with an empty first commit
                while (commit == null || !treeWalk.next()) {
                    if (!commits.hasNext()) {
                        return endOfData();
                    }
                    commit = commits.next();
                    treeWalk.reset(commit.getTree());
                }
            }
            catch (IOException e) {
                throw new TrinoException(GIT_READ_ERROR, "Failed to read the tree of commit " + commit.getName(), e);
            }
            return new TreeEntry(
                    commit.getName(),
                    treeWalk.getFileMode(),
                    treeWalk.getRawMode(0),
                    treeWalk.getObjectId(0).getName(),
                    treeWalk.getNameString(),
                    treeWalk.getPathString(),
                    treeWalk.getDepth(),
                    Suppliers.memoize(this::readAttributes));
        }

        private Map<String, String> readAttributes()
        {
            ImmutableMap.Builder<String, String> attributes = ImmutableMap.builder();
            for (Attribute attribute : treeWalk.getAttributes().getAll()) {
                attributes.put(attribute.getKey(), switch (attribute.getState()) {
                    case SET -> "true";
                    case UNSET -> "false";
                    case CUSTOM -> attribute.getValue();
                    // unspecified attributes are not returned by getAll()
                    case UNSPECIFIED -> throw new IllegalStateException("Unexpected unspecified attribute " + attribute.getKey());
                });
            }
            return attributes.buildOrThrow();
        }

        private static List<String> getValidPaths(List<String> paths)
        {
            // paths that are empty, or start or end with a slash, can't match any file
            return paths.stream()
                    .filter(path -> !path.isEmpty() && !path.startsWith("/") && !path.endsWith("/"))
                    .collect(toImmutableList());
        }

        @Override
        public void close()
        {
            treeWalk.close();
            revWalk.close();
        }
    }
}
