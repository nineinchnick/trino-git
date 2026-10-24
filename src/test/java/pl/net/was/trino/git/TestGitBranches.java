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

import io.trino.spi.connector.RecordCursor;
import io.trino.spi.predicate.TupleDomain;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jgit.lib.Constants.OBJ_BLOB;

public class TestGitBranches
{
    @Test
    public void testEmptyRepository(@TempDir Path tempDir)
            throws Exception
    {
        Path source = tempDir.resolve("source");
        Git.init().setDirectory(source.toFile()).setInitialBranch("main").call().close();

        assertThat(readBranches(source, tempDir)).isEmpty();
    }

    @Test
    public void testBranchNotPointingToCommit(@TempDir Path tempDir)
            throws Exception
    {
        Path source = tempDir.resolve("source");
        RevCommit commit;
        ObjectId blob;
        try (Git git = Git.init().setDirectory(source.toFile()).setInitialBranch("main").call();
                ObjectInserter inserter = git.getRepository().newObjectInserter()) {
            commit = git.commit().setMessage("first").setAllowEmpty(true).setSign(false).call();
            blob = inserter.insert(OBJ_BLOB, "key".getBytes(UTF_8));
            inserter.flush();
            RefUpdate update = git.getRepository().updateRef("refs/heads/blob");
            update.setNewObjectId(blob);
            assertThat(update.update()).isEqualTo(RefUpdate.Result.NEW);
        }

        assertThat(readBranches(source, tempDir)).containsExactlyInAnyOrder(
                Arrays.asList(commit.getName(), "refs/heads/main", true),
                Arrays.asList(blob.getName(), "refs/heads/blob", null));
    }

    private static List<List<Object>> readBranches(Path source, Path tempDir)
    {
        GitRepositoryManager repositoryManager = new GitRepositoryManager(new GitConfig()
                .setUrl(source.toUri())
                .setCacheDirectory(tempDir.resolve("cache").toFile()));
        GitRecordSet recordSet = new GitRecordSet(
                new GitSplit("branches", Optional.empty()),
                new GitTableHandle("default", "branches", TupleDomain.all()),
                List.of(
                        new GitColumnHandle("object_id", VARCHAR, 0),
                        new GitColumnHandle("name", VARCHAR, 1),
                        new GitColumnHandle("is_merged", BOOLEAN, 2)),
                repositoryManager.getRepository());
        List<List<Object>> rows = new ArrayList<>();
        try (RecordCursor cursor = recordSet.cursor()) {
            while (cursor.advanceNextPosition()) {
                rows.add(Arrays.asList(
                        cursor.getSlice(0).toStringUtf8(),
                        cursor.getSlice(1).toStringUtf8(),
                        cursor.isNull(2) ? null : cursor.getBoolean(2)));
            }
        }
        return rows;
    }
}
