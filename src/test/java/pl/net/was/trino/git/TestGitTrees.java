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
import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGitTrees
{
    @Test
    public void testFileModes(@TempDir Path tempDir)
            throws Exception
    {
        try (Git git = Git.init().setDirectory(tempDir.toFile()).setInitialBranch("main").call()) {
            Files.writeString(tempDir.resolve("file.txt"), "file");
            Path script = Files.writeString(tempDir.resolve("run.sh"), "#!/bin/sh");
            Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwxr-xr-x"));
            Files.createDirectories(tempDir.resolve("dir/nested"));
            Files.writeString(tempDir.resolve("dir/nested/deep.txt"), "deep");
            Files.createSymbolicLink(tempDir.resolve("link"), Path.of("file.txt"));
            git.add().addFilepattern(".").call();
            git.commit().setMessage("modes").setSign(false).call();

            List<List<Object>> rows = new ArrayList<>();
            try (RecordCursor cursor = new TreesRecordCursor(
                    List.of(
                            new GitColumnHandle("path_name", VARCHAR, 0),
                            new GitColumnHandle("file_mode", VARCHAR, 1),
                            new GitColumnHandle("mode", VARCHAR, 2),
                            new GitColumnHandle("depth", INTEGER, 3)),
                    git,
                    git.getRepository().getRefDatabase().getRefs(),
                    Optional.empty(),
                    Optional.empty())) {
                while (cursor.advanceNextPosition()) {
                    rows.add(List.of(
                            cursor.getSlice(0).toStringUtf8(),
                            cursor.getSlice(1).toStringUtf8(),
                            cursor.getSlice(2).toStringUtf8(),
                            cursor.getLong(3)));
                }
            }
            // directories come before their contents
            assertThat(rows).containsExactly(
                    List.of("dir", "directory", "040000", 0L),
                    List.of("dir/nested", "directory", "040000", 1L),
                    List.of("dir/nested/deep.txt", "file", "100644", 2L),
                    List.of("file.txt", "file", "100644", 0L),
                    List.of("link", "symlink", "120000", 0L),
                    List.of("run.sh", "executable", "100755", 0L));
        }
    }
}
