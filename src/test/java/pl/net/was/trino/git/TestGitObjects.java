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
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.RefUpdate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jgit.lib.Constants.OBJ_BLOB;

public class TestGitObjects
{
    @Test
    public void testReadsLooseAndPackedObjects(@TempDir Path tempDir)
            throws Exception
    {
        try (Git git = Git.init().setDirectory(tempDir.toFile()).setInitialBranch("main").call();
                ObjectInserter inserter = git.getRepository().newObjectInserter()) {
            Files.writeString(tempDir.resolve("packed.txt"), "packed");
            git.add().addFilepattern("packed.txt").call();
            git.commit().setMessage("packed").setSign(false).call();
            git.gc().call();

            Files.createDirectory(tempDir.resolve("dir"));
            Files.writeString(tempDir.resolve("dir/loose.txt"), "loose");
            git.add().addFilepattern("dir/loose.txt").call();
            git.commit().setMessage("loose").setSign(false).call();

            ObjectId tagged = inserter.insert(OBJ_BLOB, "tagged".getBytes(UTF_8));
            inserter.insert(OBJ_BLOB, "unreachable".getBytes(UTF_8));
            inserter.flush();
            RefUpdate update = git.getRepository().updateRef("refs/tags/blob");
            update.setNewObjectId(tagged);
            assertThat(update.update()).isEqualTo(RefUpdate.Result.NEW);

            assertThat(readObjects(git)).containsOnlyKeys(
                            blobId("packed"),
                            blobId("loose"),
                            tagged.getName())
                    .containsValues("packed", "loose", "tagged");
        }
    }

    private static Map<String, String> readObjects(Git git)
            throws IOException
    {
        Map<String, String> objects = new HashMap<>();
        try (RecordCursor cursor = new ObjectsTable().createCursor(
                List.of(new GitColumnHandle("object_id", VARCHAR, 0), new GitColumnHandle("contents", VARBINARY, 1)),
                git,
                git.getRepository().getRefDatabase().getRefs(),
                Optional.empty(),
                Optional.empty())) {
            while (cursor.advanceNextPosition()) {
                assertThat(cursor.isNull(1)).isFalse();
                objects.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
            }
        }
        return objects;
    }

    private static String blobId(String contents)
    {
        try (ObjectInserter.Formatter formatter = new ObjectInserter.Formatter()) {
            return formatter.idFor(OBJ_BLOB, contents.getBytes(UTF_8)).getName();
        }
    }
}
