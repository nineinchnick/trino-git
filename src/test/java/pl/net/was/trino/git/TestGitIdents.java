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

import io.trino.spi.block.Block;
import io.trino.spi.connector.RecordCursor;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.PersonIdent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGitIdents
{
    @Test
    public void testGroupsIdentitiesUsingMailmap(@TempDir Path tempDir)
            throws Exception
    {
        Path source = tempDir.resolve("source");
        try (Git git = Git.init().setDirectory(source.toFile()).setInitialBranch("main").call()) {
            commit(git, "joe", "joe@laptop.local");
            // linked only through the mailmap
            commit(git, "Joe Developer", "joe@example.com");
            // linked by using the same email with another name
            commit(git, "J. Developer", "joe@example.com");
            commit(git, "Ann", "ann@example.com");
            Files.writeString(source.resolve(".mailmap"), "Joe Developer <joe@example.com> <joe@laptop.local>\n");
            git.add().addFilepattern(".mailmap").call();
            commit(git, "Ann", "ann@example.com");
        }

        GitRepositoryManager repositoryManager = new GitRepositoryManager(new GitConfig()
                .setUrl(source.toUri())
                .setCacheDirectory(tempDir.resolve("cache").toFile()));
        List<List<Object>> rows = new ArrayList<>();
        try (Git repo = repositoryManager.getRepository();
                RecordCursor cursor = new IdentsRecordCursor(
                        List.of(
                                new GitColumnHandle("email", VARCHAR, 0),
                                new GitColumnHandle("name", VARCHAR, 1),
                                new GitColumnHandle("extra_emails", VARCHAR, 2),
                                new GitColumnHandle("extra_names", VARCHAR, 3)),
                        repo)) {
            while (cursor.advanceNextPosition()) {
                rows.add(List.of(
                        cursor.getSlice(0).toStringUtf8(),
                        cursor.getSlice(1).toStringUtf8(),
                        toList((Block) cursor.getObject(2)),
                        toList((Block) cursor.getObject(3))));
            }
        }
        assertThat(rows).containsExactlyInAnyOrder(
                List.of("joe@example.com", "Joe Developer", List.of("joe@laptop.local"), List.of("J. Developer", "joe")),
                List.of("ann@example.com", "Ann", List.of(), List.of()));
    }

    private static void commit(Git git, String name, String email)
            throws Exception
    {
        PersonIdent ident = new PersonIdent(name, email, Instant.ofEpochSecond(1_600_000_000L), UTC);
        git.commit().setMessage(name).setAllowEmpty(true).setAuthor(ident).setCommitter(ident).setSign(false).call();
    }

    private static List<String> toList(Block block)
    {
        return IntStream.range(0, block.getPositionCount())
                .mapToObj(position -> VARCHAR.getSlice(block, position).toStringUtf8())
                .toList();
    }
}
