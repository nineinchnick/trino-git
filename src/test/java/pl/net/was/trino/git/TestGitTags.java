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
import org.eclipse.jgit.lib.TagBuilder;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jgit.lib.Constants.OBJ_BLOB;
import static org.eclipse.jgit.lib.Constants.OBJ_COMMIT;
import static org.eclipse.jgit.lib.Constants.OBJ_TAG;

public class TestGitTags
{
    @Test
    public void testTagsPointingToAnyObject(@TempDir Path tempDir)
            throws Exception
    {
        Path source = tempDir.resolve("source");
        RevCommit commit;
        ObjectId blob;
        ObjectId tagWithoutTagger;
        ObjectId tagOfTag;
        try (Git git = Git.init().setDirectory(source.toFile()).setInitialBranch("main").call();
                ObjectInserter inserter = git.getRepository().newObjectInserter()) {
            commit = git.commit().setMessage("first").setAllowEmpty(true).setSign(false).call();
            blob = inserter.insert(OBJ_BLOB, "key".getBytes(UTF_8));
            // tags made before git recorded the tagger
            tagWithoutTagger = inserter.insert(tag(commit, OBJ_COMMIT, "old"));
            tagOfTag = inserter.insert(tag(tagWithoutTagger, OBJ_TAG, "nested"));
            inserter.flush();
            createRef(git, "refs/tags/blob", blob);
            createRef(git, "refs/tags/old", tagWithoutTagger);
            createRef(git, "refs/tags/nested", tagOfTag);
        }

        GitRepositoryManager repositoryManager = new GitRepositoryManager(new GitConfig()
                .setUrl(source.toUri())
                .setCacheDirectory(tempDir.resolve("cache").toFile()));
        GitRecordSet recordSet = new GitRecordSet(
                new GitSplit("tags", Optional.empty()),
                new GitTableHandle("default", "tags", TupleDomain.all()),
                List.of(
                        new GitColumnHandle("object_id", VARCHAR, 0),
                        new GitColumnHandle("commit_id", VARCHAR, 1),
                        new GitColumnHandle("name", VARCHAR, 2),
                        new GitColumnHandle("tag_time", TIMESTAMP_TZ_SECONDS, 3)),
                repositoryManager.getRepository());
        List<List<Object>> rows = new ArrayList<>();
        try (RecordCursor cursor = recordSet.cursor()) {
            while (cursor.advanceNextPosition()) {
                rows.add(Arrays.asList(
                        cursor.getSlice(0).toStringUtf8(),
                        cursor.isNull(1) ? null : cursor.getSlice(1).toStringUtf8(),
                        cursor.getSlice(2).toStringUtf8(),
                        cursor.isNull(3) ? null : cursor.getLong(3)));
            }
        }
        assertThat(rows).containsExactlyInAnyOrder(
                Arrays.asList(blob.getName(), null, "refs/tags/blob", null),
                Arrays.asList(tagWithoutTagger.getName(), commit.getName(), "refs/tags/old", null),
                Arrays.asList(tagOfTag.getName(), commit.getName(), "refs/tags/nested", null));
    }

    private static TagBuilder tag(ObjectId target, int type, String name)
    {
        TagBuilder tag = new TagBuilder();
        tag.setObjectId(target, type);
        tag.setTag(name);
        tag.setMessage(name);
        return tag;
    }

    private static void createRef(Git git, String name, ObjectId target)
            throws Exception
    {
        RefUpdate update = git.getRepository().updateRef(name);
        update.setNewObjectId(target);
        assertThat(update.update()).isEqualTo(RefUpdate.Result.NEW);
    }
}
