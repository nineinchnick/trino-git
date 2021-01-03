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
import org.eclipse.jgit.api.errors.GitAPIException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestGitRecordSet
{
    private static final URI uri = URI.create("fake.invalid");

    @BeforeMethod
    public void setUp()
            throws IOException, GitAPIException
    {
        TestGitClient.setupRepo(uri);
    }

    @Test
    public void testGetColumnTypes()
    {
        RecordSet recordSet = new GitRecordSet(new GitSplit("commits", uri), List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("author_name", createUnboundedVarcharType(), 1)));
        assertEquals(recordSet.getColumnTypes(), List.of(createUnboundedVarcharType(), createUnboundedVarcharType()));

        recordSet = new GitRecordSet(new GitSplit("commits", uri), List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 1),
                new GitColumnHandle("author_name", createUnboundedVarcharType(), 0)));
        assertEquals(recordSet.getColumnTypes(), List.of(createUnboundedVarcharType(), createUnboundedVarcharType()));

        recordSet = new GitRecordSet(new GitSplit("commits", uri), List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 1),
                new GitColumnHandle("author_name", createUnboundedVarcharType(), 1),
                new GitColumnHandle("author_email", createUnboundedVarcharType(), 0)));
        assertEquals(recordSet.getColumnTypes(), List.of(createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType()));

        recordSet = new GitRecordSet(new GitSplit("commits", uri), List.of());
        assertEquals(recordSet.getColumnTypes(), List.of());
    }

    @Test
    public void testCommitsCursorSimple()
    {
        RecordSet recordSet = new GitRecordSet(new GitSplit("commits", uri), List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("author_name", createUnboundedVarcharType(), 1)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());
        assertEquals(cursor.getType(1), createUnboundedVarcharType());

        Map<String, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
        }
        assertEquals(data, Map.of(
                "080dfdf0aac7d302dc31d57f62942bb6533944f7", "test",
                "c3b14e59f88d0d6597b98ee93cf61e7556d540a4", "test"));
    }

    @Test
    public void testBranchesCursorSimple()
    {
        RecordSet recordSet = new GitRecordSet(new GitSplit("branches", uri), List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("name", createUnboundedVarcharType(), 1)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());
        assertEquals(cursor.getType(1), createUnboundedVarcharType());

        Map<String, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
        }
        assertEquals(data, Map.of("c3b14e59f88d0d6597b98ee93cf61e7556d540a4", "refs/heads/master"));
    }

    @Test
    public void testTagsCursorSimple()
    {
        RecordSet recordSet = new GitRecordSet(new GitSplit("tags", uri), List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("name", createUnboundedVarcharType(), 1)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());
        assertEquals(cursor.getType(1), createUnboundedVarcharType());

        Map<String, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
        }
        assertEquals(data, Map.of("7afcc1aaeab61c3fd7f2b1b5df5178a823cbf77e", "refs/tags/tag_for_testing"));
    }
}
