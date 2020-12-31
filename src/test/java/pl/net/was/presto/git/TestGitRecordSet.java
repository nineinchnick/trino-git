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
package pl.net.was.presto.git;

import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestGitRecordSet
{
    private static final URI uri = URI.create("fake.invalid");

    @BeforeMethod
    public void setUp()
            throws IOException, GitAPIException, URISyntaxException
    {
        File localPath;
        try {
            localPath = GitRecordSet.ensureDir(uri.toString());
        }
        catch (IOException ignored) {
            return;
        }
        if (localPath.exists()) {
            Files.walk(localPath.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        Git.init().setDirectory(localPath).call();
    }

    @Test
    public void testGetColumnTypes()
    {
        RecordSet recordSet = new GitRecordSet(new GitSplit("commits", uri), List.of(
                new GitColumnHandle("text", createUnboundedVarcharType(), 0),
                new GitColumnHandle("value", BIGINT, 1)));
        assertEquals(recordSet.getColumnTypes(), List.of(createUnboundedVarcharType(), BIGINT));

        recordSet = new GitRecordSet(new GitSplit("commits", uri), List.of(
                new GitColumnHandle("value", BIGINT, 1),
                new GitColumnHandle("text", createUnboundedVarcharType(), 0)));
        assertEquals(recordSet.getColumnTypes(), List.of(BIGINT, createUnboundedVarcharType()));

        recordSet = new GitRecordSet(new GitSplit("commits", uri), List.of(
                new GitColumnHandle("value", BIGINT, 1),
                new GitColumnHandle("value", BIGINT, 1),
                new GitColumnHandle("text", createUnboundedVarcharType(), 0)));
        assertEquals(recordSet.getColumnTypes(), List.of(BIGINT, BIGINT, createUnboundedVarcharType()));

        recordSet = new GitRecordSet(new GitSplit("commits", uri), List.of());
        assertEquals(recordSet.getColumnTypes(), List.of());
    }

    @Test
    public void testCommitsCursorSimple()
    {
        RecordSet recordSet = new GitRecordSet(new GitSplit("commits", uri), List.of(
                new GitColumnHandle("text", createUnboundedVarcharType(), 0),
                new GitColumnHandle("value", BIGINT, 1)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());
        assertEquals(cursor.getType(1), BIGINT);

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(1));
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
        }
        assertEquals(data, Map.of("eleven", 11L, "twelve", 12L));
    }

    @Test
    public void testBranchesCursorSimple()
    {
        RecordSet recordSet = new GitRecordSet(new GitSplit("branches", uri), List.of(
                new GitColumnHandle("text", createUnboundedVarcharType(), 0),
                new GitColumnHandle("value", createUnboundedVarcharType(), 1)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());
        assertEquals(cursor.getType(1), createUnboundedVarcharType());

        Map<String, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
        }
        assertEquals(data, Map.of("two", "2", "three", "3"));
    }
}
