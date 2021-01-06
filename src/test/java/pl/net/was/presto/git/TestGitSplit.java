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
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestGitSplit
{
    private final GitSplit split = new GitSplit("tableName", new URI("url.invalid"));

    public TestGitSplit()
            throws URISyntaxException
    {}

    @Test
    public void testAddresses()
            throws URISyntaxException
    {
        URI testURI = new URI("url.invalid");
        GitSplit httpSplit = new GitSplit("tableName", testURI);
        assertTrue(httpSplit.isRemotelyAccessible());
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<GitSplit> codec = jsonCodec(GitSplit.class);
        String json = codec.toJson(split);
        GitSplit copy = codec.fromJson(json);
        assertEquals(copy.getTableName(), split.getTableName());

        assertTrue(copy.isRemotelyAccessible());
    }
}
