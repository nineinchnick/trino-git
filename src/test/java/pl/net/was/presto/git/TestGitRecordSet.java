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
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.VarbinaryType;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.testng.annotations.BeforeSuite;
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

    @BeforeSuite
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
                new GitColumnHandle("author_name", createUnboundedVarcharType(), 1),
                new GitColumnHandle("commit_time", TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS, 2)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());
        assertEquals(cursor.getType(1), createUnboundedVarcharType());

        Map<String, List<Object>> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), List.of(
                    cursor.getSlice(1).toStringUtf8(),
                    cursor.getLong(2)));
        }
        assertEquals(data, Map.of(
                "080dfdf0aac7d302dc31d57f62942bb6533944f7", List.of("test", 6475355394048000L),
                "c3b14e59f88d0d6597b98ee93cf61e7556d540a4", List.of("test", 6475355394048000L)));
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

    @Test
    public void testTreesCursorSimple()
    {
        RecordSet recordSet = new GitRecordSet(new GitSplit("trees", uri), List.of(
                new GitColumnHandle("commit_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 1)));
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
                "080dfdf0aac7d302dc31d57f62942bb6533944f7", "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
                "c3b14e59f88d0d6597b98ee93cf61e7556d540a4", "5dd01c177f5d7d1be5346a5bc18a569a7410c2ef"));
    }

    @Test
    public void testObjectsCursorSimple()
    {
        RecordSet recordSet = new GitRecordSet(new GitSplit("objects", uri), List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("contents", VarbinaryType.VARBINARY, 1)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());
        assertEquals(cursor.getType(1), VarbinaryType.VARBINARY);

        Map<String, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
            String objectId = cursor.getSlice(0).toStringUtf8();
            String contents = cursor.getSlice(1).toStringUtf8();
            data.put(objectId, contents);
        }
        assertEquals(data, Map.of(
                "5dd01c177f5d7d1be5346a5bc18a569a7410c2ef", "Hello, world!",
                "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", ""));
    }

    @Test
    public void testDiffStatsCursorSimple()
    {
        RecordSet recordSet = new GitRecordSet(new GitSplit("diff_stats", uri), List.of(
                new GitColumnHandle("commit_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("old_commit_id", createUnboundedVarcharType(), 1),
                new GitColumnHandle("added_lines", IntegerType.INTEGER, 2),
                new GitColumnHandle("deleted_lines", IntegerType.INTEGER, 3)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());
        assertEquals(cursor.getType(1), createUnboundedVarcharType());
        assertEquals(cursor.getType(2), IntegerType.INTEGER);
        assertEquals(cursor.getType(3), IntegerType.INTEGER);

        Map<String, List<Object>> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
            assertFalse(cursor.isNull(2));
            assertFalse(cursor.isNull(3));
            data.put(
                    cursor.getSlice(0).toStringUtf8(),
                    List.of(
                            cursor.getSlice(1).toStringUtf8(),
                            cursor.getLong(2),
                            cursor.getLong(3)));
        }
        assertEquals(data, Map.of(
                "c3b14e59f88d0d6597b98ee93cf61e7556d540a4",
                List.of(
                        "080dfdf0aac7d302dc31d57f62942bb6533944f7",
                        1L,
                        0L)));
    }
}
