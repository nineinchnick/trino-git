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

import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestGitTableHandle
{
    private final GitTableHandle tableHandle = new GitTableHandle("schemaName", "tableName");

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<GitTableHandle> codec = jsonCodec(GitTableHandle.class);
        String json = codec.toJson(tableHandle);
        GitTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new GitTableHandle("schema", "table"),
                        new GitTableHandle("schema", "table"))
                .addEquivalentGroup(
                        new GitTableHandle("schemaX", "table"),
                        new GitTableHandle("schemaX", "table"))
                .addEquivalentGroup(
                        new GitTableHandle("schema", "tableX"),
                        new GitTableHandle("schema", "tableX"))
                .check();
    }
}
