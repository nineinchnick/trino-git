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

import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGitTableHandle
{
    private final GitTableHandle tableHandle = new GitTableHandle("schemaName", "tableName", Optional.empty(), OptionalLong.empty());

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<GitTableHandle> codec = jsonCodec(GitTableHandle.class);
        String json = codec.toJson(tableHandle);
        GitTableHandle copy = codec.fromJson(json);
        assertThat(copy).isEqualTo(tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new GitTableHandle("schema", "table", Optional.empty(), OptionalLong.empty()),
                        new GitTableHandle("schema", "table", Optional.empty(), OptionalLong.empty()))
                .addEquivalentGroup(
                        new GitTableHandle("schemaX", "table", Optional.empty(), OptionalLong.empty()),
                        new GitTableHandle("schemaX", "table", Optional.empty(), OptionalLong.empty()))
                .addEquivalentGroup(
                        new GitTableHandle("schema", "tableX", Optional.empty(), OptionalLong.empty()),
                        new GitTableHandle("schema", "tableX", Optional.empty(), OptionalLong.empty()))
                .check();
    }
}
