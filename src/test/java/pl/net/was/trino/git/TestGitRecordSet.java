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
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarbinaryType;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGitRecordSet
{
    private static final URI uri = URI.create("fake.invalid");

    @BeforeAll
    public static void setUp()
            throws IOException, GitAPIException
    {
        TestGitClient.setupRepo(uri);
    }

    @Test
    public void testGetColumnTypes()
    {
        GitSplit split = new GitSplit("commits", uri, Optional.empty());
        GitTableHandle table = new GitTableHandle("default", "commits", Optional.empty(), OptionalLong.empty());
        RecordSet recordSet = new GitRecordSet(split, table, List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("author_name", createUnboundedVarcharType(), 1)));
        assertThat(recordSet.getColumnTypes()).isEqualTo(List.of(createUnboundedVarcharType(), createUnboundedVarcharType()));

        recordSet = new GitRecordSet(split, table, List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 1),
                new GitColumnHandle("author_name", createUnboundedVarcharType(), 0)));
        assertThat(recordSet.getColumnTypes()).isEqualTo(List.of(createUnboundedVarcharType(), createUnboundedVarcharType()));

        recordSet = new GitRecordSet(split, table, List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 1),
                new GitColumnHandle("author_name", createUnboundedVarcharType(), 1),
                new GitColumnHandle("author_email", createUnboundedVarcharType(), 0)));
        assertThat(recordSet.getColumnTypes()).isEqualTo(List.of(createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType()));

        recordSet = new GitRecordSet(split, table, List.of());
        assertThat(recordSet.getColumnTypes()).isEmpty();
    }

    @Test
    public void testCommitsCursorSimple()
    {
        GitSplit split = new GitSplit("commits", uri, Optional.empty());
        GitTableHandle table = new GitTableHandle("default", "commits", Optional.empty(), OptionalLong.empty());
        RecordSet recordSet = new GitRecordSet(split, table, List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("author_name", createUnboundedVarcharType(), 1),
                new GitColumnHandle("commit_time", TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS, 2)));
        try (RecordCursor cursor = recordSet.cursor()) {
            assertThat(cursor.getType(0)).isEqualTo(createUnboundedVarcharType());
            assertThat(cursor.getType(1)).isEqualTo(createUnboundedVarcharType());

            Map<String, List<Object>> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                data.put(cursor.getSlice(0).toStringUtf8(), List.of(
                        cursor.getSlice(1).toStringUtf8(),
                        cursor.getLong(2)));
            }
            assertThat(data).isEqualTo(Map.of(
                    "080dfdf0aac7d302dc31d57f62942bb6533944f7", List.of("test", 6475355394048000L),
                    "c3b14e59f88d0d6597b98ee93cf61e7556d540a4", List.of("test", 6475355394048000L)));
        }
    }

    @Test
    public void testBranchesCursorSimple()
    {
        GitSplit split = new GitSplit("branches", uri, Optional.empty());
        GitTableHandle table = new GitTableHandle("default", "branches", Optional.empty(), OptionalLong.empty());
        RecordSet recordSet = new GitRecordSet(split, table, List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("name", createUnboundedVarcharType(), 1)));
        try (RecordCursor cursor = recordSet.cursor()) {
            assertThat(cursor.getType(0)).isEqualTo(createUnboundedVarcharType());
            assertThat(cursor.getType(1)).isEqualTo(createUnboundedVarcharType());

            Map<String, String> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                assertThat(cursor.isNull(0)).isFalse();
                assertThat(cursor.isNull(1)).isFalse();
                data.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
            }
            assertThat(data).isEqualTo(Map.of("c3b14e59f88d0d6597b98ee93cf61e7556d540a4", "refs/heads/master"));
        }
    }

    @Test
    public void testTagsCursorSimple()
    {
        GitSplit split = new GitSplit("tags", uri, Optional.empty());
        GitTableHandle table = new GitTableHandle("default", "tags", Optional.empty(), OptionalLong.empty());
        RecordSet recordSet = new GitRecordSet(split, table, List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("name", createUnboundedVarcharType(), 1)));
        try (RecordCursor cursor = recordSet.cursor()) {
            assertThat(cursor.getType(0)).isEqualTo(createUnboundedVarcharType());
            assertThat(cursor.getType(1)).isEqualTo(createUnboundedVarcharType());

            Map<String, String> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                assertThat(cursor.isNull(0)).isFalse();
                assertThat(cursor.isNull(1)).isFalse();
                data.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
            }
            assertThat(data).isEqualTo(Map.of("7afcc1aaeab61c3fd7f2b1b5df5178a823cbf77e", "refs/tags/tag_for_testing"));
        }
    }

    @Test
    public void testTagsCursorWithTagTime()
    {
        GitSplit split = new GitSplit("tags", uri, Optional.empty());
        GitTableHandle table = new GitTableHandle("default", "tags", Optional.empty(), OptionalLong.empty());
        RecordSet recordSet = new GitRecordSet(split, table, List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("name", createUnboundedVarcharType(), 1),
                new GitColumnHandle("tag_time", TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS, 2)));
        try (RecordCursor cursor = recordSet.cursor()) {
            assertThat(cursor.getType(0)).isEqualTo(createUnboundedVarcharType());
            assertThat(cursor.getType(1)).isEqualTo(createUnboundedVarcharType());
            assertThat(cursor.getType(2)).isEqualTo(TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS);

            Map<String, List<?>> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                assertThat(cursor.isNull(0)).isFalse();
                assertThat(cursor.isNull(1)).isFalse();
                assertThat(cursor.isNull(2)).isFalse();
                data.put(cursor.getSlice(0).toStringUtf8(), List.of(
                        cursor.getSlice(1).toStringUtf8(),
                        cursor.getLong(2)));
            }
            assertThat(data).isEqualTo(Map.of("7afcc1aaeab61c3fd7f2b1b5df5178a823cbf77e", List.of("refs/tags/tag_for_testing", 1580897313000000L)));
        }
    }

    @Test
    public void testTreesCursorSimple()
    {
        GitSplit split = new GitSplit("trees", uri, Optional.empty());
        GitTableHandle table = new GitTableHandle("default", "trees", Optional.empty(), OptionalLong.empty());
        RecordSet recordSet = new GitRecordSet(split, table, List.of(
                new GitColumnHandle("commit_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 1)));
        try (RecordCursor cursor = recordSet.cursor()) {
            assertThat(cursor.getType(0)).isEqualTo(createUnboundedVarcharType());
            assertThat(cursor.getType(1)).isEqualTo(createUnboundedVarcharType());

            Map<String, String> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                assertThat(cursor.isNull(0)).isFalse();
                assertThat(cursor.isNull(1)).isFalse();
                data.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
            }
            assertThat(data).isEqualTo(Map.of(
                    "080dfdf0aac7d302dc31d57f62942bb6533944f7", "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
                    "c3b14e59f88d0d6597b98ee93cf61e7556d540a4", "5dd01c177f5d7d1be5346a5bc18a569a7410c2ef"));
        }
    }

    @Test
    public void testObjectsCursorSimple()
    {
        GitSplit split = new GitSplit("objects", uri, Optional.empty());
        GitTableHandle table = new GitTableHandle("default", "objects", Optional.empty(), OptionalLong.empty());
        RecordSet recordSet = new GitRecordSet(split, table, List.of(
                new GitColumnHandle("object_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("contents", VarbinaryType.VARBINARY, 1)));
        try (RecordCursor cursor = recordSet.cursor()) {
            assertThat(cursor.getType(0)).isEqualTo(createUnboundedVarcharType());
            assertThat(cursor.getType(1)).isEqualTo(VarbinaryType.VARBINARY);

            Map<String, String> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                assertThat(cursor.isNull(0)).isFalse();
                assertThat(cursor.isNull(1)).isFalse();
                String objectId = cursor.getSlice(0).toStringUtf8();
                String contents = cursor.getSlice(1).toStringUtf8();
                data.put(objectId, contents);
            }
            assertThat(data).isEqualTo(Map.of(
                    "5dd01c177f5d7d1be5346a5bc18a569a7410c2ef", "Hello, world!",
                    "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", ""));
        }
    }

    @Test
    public void testDiffStatsCursorSimple()
    {
        GitSplit split = new GitSplit("diff_stats", uri, Optional.empty());
        GitTableHandle table = new GitTableHandle("default", "diff_stats", Optional.empty(), OptionalLong.empty());
        RecordSet recordSet = new GitRecordSet(split, table, List.of(
                new GitColumnHandle("commit_id", createUnboundedVarcharType(), 0),
                new GitColumnHandle("old_commit_id", createUnboundedVarcharType(), 1),
                new GitColumnHandle("added_lines", IntegerType.INTEGER, 2),
                new GitColumnHandle("deleted_lines", IntegerType.INTEGER, 3)));
        try (RecordCursor cursor = recordSet.cursor()) {
            assertThat(cursor.getType(0)).isEqualTo(createUnboundedVarcharType());
            assertThat(cursor.getType(1)).isEqualTo(createUnboundedVarcharType());
            assertThat(cursor.getType(2)).isEqualTo(IntegerType.INTEGER);
            assertThat(cursor.getType(3)).isEqualTo(IntegerType.INTEGER);

            Map<String, List<Object>> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                assertThat(cursor.isNull(0)).isFalse();
                assertThat(cursor.isNull(1)).isFalse();
                assertThat(cursor.isNull(2)).isFalse();
                assertThat(cursor.isNull(3)).isFalse();
                data.put(
                        cursor.getSlice(0).toStringUtf8(),
                        List.of(
                                cursor.getSlice(1).toStringUtf8(),
                                cursor.getLong(2),
                                cursor.getLong(3)));
            }
            assertThat(data).isEqualTo(Map.of(
                    "c3b14e59f88d0d6597b98ee93cf61e7556d540a4",
                    List.of(
                            "080dfdf0aac7d302dc31d57f62942bb6533944f7",
                            1L,
                            0L)));
        }
    }
}
