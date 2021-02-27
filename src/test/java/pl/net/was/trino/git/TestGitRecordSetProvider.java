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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestGitRecordSetProvider
{
    private static final URI uri = URI.create("fake.invalid");

    @BeforeSuite
    public void setUp()
            throws IOException, GitAPIException
    {
        TestGitClient.setupRepo(uri);
    }

    @Test
    public void testGetRecordSet()
    {
        GitRecordSetProvider recordSetProvider = new GitRecordSetProvider();
        RecordSet recordSet = recordSetProvider.getRecordSet(
                GitTransactionHandle.INSTANCE,
                SESSION,
                new GitSplit("commits", uri, Optional.empty()),
                new GitTableHandle("default", "commits", Optional.empty(), OptionalLong.empty()),
                List.of(
                        new GitColumnHandle("object_id", createUnboundedVarcharType(), 0),
                        new GitColumnHandle("author_name", createUnboundedVarcharType(), 1)));
        assertNotNull(recordSet, "recordSet is null");

        RecordCursor cursor = recordSet.cursor();
        assertNotNull(cursor, "cursor is null");

        Map<String, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
        }
        assertEquals(data, ImmutableMap.<String, String>builder()
                .put("080dfdf0aac7d302dc31d57f62942bb6533944f7", "test")
                .put("c3b14e59f88d0d6597b98ee93cf61e7556d540a4", "test")
                .build());
    }
}
