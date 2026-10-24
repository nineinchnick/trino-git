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

import org.junit.jupiter.api.Test;
import pl.net.was.trino.git.Mailmap.Identity;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMailmap
{
    private static final Mailmap MAILMAP = Mailmap.parse(
            """
            # comments and empty lines are ignored

            Proper Name <name@example.com>
            <proper@example.com> <old@example.com>
            Joe Developer <joe@example.com> <joe@laptop.local> # trailing comment
            Jane Doe <jane@example.com> jane <JANE@desktop.local>
            Other Jane <other@example.com> <jane@desktop.local>
            """);

    @Test
    public void testReplacesName()
    {
        assertThat(MAILMAP.map(new Identity("name", "name@example.com"))).isEqualTo(new Identity("Proper Name", "name@example.com"));
    }

    @Test
    public void testReplacesEmail()
    {
        assertThat(MAILMAP.map(new Identity("Someone", "old@example.com"))).isEqualTo(new Identity("Someone", "proper@example.com"));
    }

    @Test
    public void testReplacesNameAndEmail()
    {
        assertThat(MAILMAP.map(new Identity("joe", "joe@laptop.local"))).isEqualTo(new Identity("Joe Developer", "joe@example.com"));
    }

    @Test
    public void testMatchesCommitNameFirst()
    {
        // emails and names are matched ignoring case
        assertThat(MAILMAP.map(new Identity("Jane", "jane@Desktop.local"))).isEqualTo(new Identity("Jane Doe", "jane@example.com"));
        assertThat(MAILMAP.map(new Identity("Somebody", "jane@desktop.local"))).isEqualTo(new Identity("Other Jane", "other@example.com"));
    }

    @Test
    public void testKeepsUnknownIdentities()
    {
        Identity identity = new Identity("Unknown", "unknown@example.com");
        assertThat(MAILMAP.map(identity)).isEqualTo(identity);
        assertThat(Mailmap.EMPTY.map(identity)).isEqualTo(identity);
    }
}
