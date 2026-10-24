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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.trino.git.TestingGitRepository.BINARY;
import static pl.net.was.trino.git.TestingGitRepository.DELETED;
import static pl.net.was.trino.git.TestingGitRepository.FEATURE_BRANCH;
import static pl.net.was.trino.git.TestingGitRepository.FEATURE_FILE;
import static pl.net.was.trino.git.TestingGitRepository.GUIDE;
import static pl.net.was.trino.git.TestingGitRepository.README;
import static pl.net.was.trino.git.TestingGitRepository.RENAMED_NEW;
import static pl.net.was.trino.git.TestingGitRepository.RENAMED_OLD;

final class TestGitConnectorQueries
        extends AbstractTestQueryFramework
{
    private TestingGitRepository repository;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        repository = closeAfterClass(new TestingGitRepository());
        return GitQueryRunner.builder(repository.getUri()).build();
    }

    @Test
    void testShowTables()
    {
        assertThat(query("SHOW TABLES"))
                .skippingTypesCheck()
                .matches("VALUES 'branches', 'commits', 'diff_stats', 'idents', 'objects', 'tags', 'trees'");
    }

    @Test
    void testColumnComments()
    {
        assertThat(query("SELECT comment FROM information_schema.columns WHERE table_name = 'commits' AND column_name = 'message'"))
                .skippingTypesCheck()
                .matches("VALUES 'Full commit message'");
        assertThat(query("SELECT count(*) FROM information_schema.columns WHERE table_schema = 'default' AND table_name != 'commit_stats' AND comment IS NULL"))
                .matches("VALUES BIGINT '0'");
    }

    @Test
    void testAuthorTime()
    {
        // the fixture makes every change 30 minutes before committing it
        assertThat(query("SELECT DISTINCT date_diff('minute', author_time, commit_time) FROM commits"))
                .matches("VALUES BIGINT '30'");
        assertThat(query("SELECT author_time FROM commits WHERE object_id = '%s'".formatted(id(repository.getInitialCommit()))))
                .matches("VALUES TIMESTAMP '2024-01-01 15:00:00 +05:30'");
    }

    @Test
    void testPathPushdown()
    {
        // pushdown must not change results, compared with the same filter that can't be pushed down
        for (String table : List.of("trees", "diff_stats")) {
            for (String path : List.of(GUIDE, RENAMED_NEW, "docs", "missing.txt", "/invalid")) {
                assertThat(query("SELECT * FROM %s WHERE path_name = '%s'".formatted(table, path)))
                        .matches("SELECT * FROM %s WHERE path_name || '' = '%s'".formatted(table, path));
            }
        }
        // renames are still detected
        assertThat(query("SELECT change_type FROM diff_stats WHERE path_name = '%s'".formatted(RENAMED_NEW)))
                .skippingTypesCheck()
                .matches("VALUES 'renamed'");
        assertQueryStats(
                getSession(),
                "SELECT * FROM trees WHERE path_name = '%s'".formatted(GUIDE),
                queryStats -> assertThat(queryStats.getPhysicalInputPositions()).isLessThan((long) computeScalar("SELECT count(*) FROM trees")),
                _ -> {});
    }

    @Test
    void testObjectIdPushdown()
            throws IOException
    {
        String blobId = blobId(repository.getInitialCommit(), README);
        assertThat(query("SELECT object_id FROM objects WHERE object_id = '%s'".formatted(blobId)))
                .skippingTypesCheck()
                .matches("VALUES '%s'".formatted(blobId));
        // ids of missing objects and of commits don't match any blob
        assertThat(query("SELECT count(*) FROM objects WHERE object_id IN ('0123456789012345678901234567890123456789', '%s')".formatted(id(repository.getInitialCommit()))))
                .matches("VALUES BIGINT '0'");
        assertQueryStats(
                getSession(),
                "SELECT * FROM objects WHERE object_id = '%s'".formatted(blobId),
                queryStats -> assertThat(queryStats.getPhysicalInputPositions()).isEqualTo(1),
                _ -> {});
    }

    @Test
    void testCommits()
    {
        assertThat(query("SELECT object_id, author_name, author_email, committer_name, committer_email, message, tree_id, commit_time FROM commits"))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            ('%s', 'Alice', 'alice@example.com', 'Bob Builder', 'bob@example.com', 'Initial commit', '%s', TIMESTAMP '2024-01-01 15:30:00 +05:30'),
                            ('%s', 'Alice Example', 'alice@example.com', 'Bob Builder', 'bob@example.com', 'Describe the repository', '%s', TIMESTAMP '2024-01-01 16:30:00 +05:30'),
                            ('%s', 'Carol', 'carol@example.com', 'Bob Builder', 'bob@example.com', 'Add a feature', '%s', TIMESTAMP '2024-01-01 17:30:00 +05:30'),
                            ('%s', 'alice', 'alice@example.com', 'Bob Builder', 'bob@example.com', 'Rename a file and add a binary file', '%s', TIMESTAMP '2024-01-01 18:30:00 +05:30'),
                            ('%s', 'Alice', 'alice@example.com', 'Bob Builder', 'bob@example.com', 'Delete a file', '%s', TIMESTAMP '2024-01-01 19:30:00 +05:30'),
                            ('%s', 'Alice', 'alice@example.com', 'Bob Builder', 'bob@example.com', 'Merge branch ''feature''', '%s', TIMESTAMP '2024-01-01 20:30:00 +05:30'),
                            ('%s', 'Alice', 'alice@example.com', 'Bob Builder', 'bob@example.com', 'Empty commit', '%s', TIMESTAMP '2024-01-01 22:30:00 +05:30'),
                            ('%s', 'Carol', 'carol@example.com', 'Bob Builder', 'bob@example.com', 'Start an orphan branch with an empty tree', '%s', TIMESTAMP '2024-01-01 23:30:00 +05:30')
                        """.formatted(
                                id(repository.getInitialCommit()),
                                treeId(repository.getInitialCommit()),
                                id(repository.getUpdateCommit()),
                                treeId(repository.getUpdateCommit()),
                                id(repository.getFeatureCommit()),
                                treeId(repository.getFeatureCommit()),
                                id(repository.getRenameCommit()),
                                treeId(repository.getRenameCommit()),
                                id(repository.getDeleteCommit()),
                                treeId(repository.getDeleteCommit()),
                                id(repository.getMergeCommit()),
                                treeId(repository.getMergeCommit()),
                                id(repository.getEmptyCommit()),
                                treeId(repository.getEmptyCommit()),
                                id(repository.getOrphanCommit()),
                                treeId(repository.getOrphanCommit())));
    }

    @Test
    void testCommitsFilterByObjectId()
    {
        assertThat(query("SELECT object_id, message FROM commits WHERE object_id = '%s'".formatted(id(repository.getMergeCommit()))))
                .skippingTypesCheck()
                .matches("VALUES ('%s', 'Merge branch ''feature''')".formatted(id(repository.getMergeCommit())));
    }

    @Test
    void testCommitsParents()
    {
        assertThat(query("SELECT object_id, parents FROM commits WHERE object_id IN ('%s', '%s', '%s')".formatted(
                id(repository.getInitialCommit()),
                id(repository.getUpdateCommit()),
                id(repository.getMergeCommit()))))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            ('%s', CAST(ARRAY[] AS array(varchar))),
                            ('%s', ARRAY['%s']),
                            ('%s', ARRAY['%s', '%s'])
                        """.formatted(
                                id(repository.getInitialCommit()),
                                id(repository.getUpdateCommit()),
                                id(repository.getInitialCommit()),
                                id(repository.getMergeCommit()),
                                id(repository.getDeleteCommit()),
                                id(repository.getFeatureCommit())));
    }

    @Test
    void testCommitsFilterByUppercaseObjectId()
    {
        assertThat(query("SELECT object_id FROM commits WHERE object_id = '%s'".formatted(id(repository.getMergeCommit()).toUpperCase(ENGLISH))))
                .returnsEmptyResult();
    }

    @Test
    void testCommitsFilterByAbbreviatedObjectId()
    {
        assertThat(query("SELECT object_id FROM commits WHERE object_id = '%s'".formatted(id(repository.getMergeCommit()).substring(0, 7))))
                .returnsEmptyResult();
    }

    @Test
    void testCommitsCombinedFilters()
    {
        assertThat(query(
                """
                SELECT object_id
                FROM commits
                WHERE object_id IN ('%s', '%s')
                AND object_id IN (SELECT object_id FROM commits WHERE message = 'Empty commit')
                """.formatted(id(repository.getInitialCommit()), id(repository.getEmptyCommit()))))
                .skippingTypesCheck()
                .matches("VALUES '%s'".formatted(id(repository.getEmptyCommit())));
    }

    @Test
    void testBranches()
    {
        assertThat(query("SELECT name, object_id, is_merged FROM branches"))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            ('refs/heads/main', '%s', true),
                            ('refs/heads/feature', '%s', true),
                            ('refs/heads/gh-pages', '%s', false)
                        """.formatted(
                                id(repository.getEmptyCommit()),
                                id(repository.getFeatureCommit()),
                                id(repository.getOrphanCommit())));
    }

    @Test
    void testRefreshProcedure(@TempDir Path cacheDirectory)
            throws Exception
    {
        try (TestingGitRepository upstream = new TestingGitRepository()) {
            getQueryRunner().createCatalog("git_refreshed", "git", ImmutableMap.of(
                    "git.url", upstream.getUri().toString(),
                    "git.cache-directory", cacheDirectory.toString(),
                    "git.fetch-interval", "1h"));
            String mainBranch = "SELECT object_id FROM git_refreshed.default.branches WHERE name = 'refs/heads/main'";
            RevCommit oldCommit = repository.getEmptyCommit();
            assertThat(query(mainBranch)).skippingTypesCheck().matches("VALUES '%s'".formatted(id(oldCommit)));

            RevCommit newCommit = upstream.addEmptyCommit("A commit pushed after the clone");
            assertThat(query(mainBranch)).skippingTypesCheck().matches("VALUES '%s'".formatted(id(oldCommit)));

            assertUpdate("CALL git_refreshed.system.refresh()");
            assertThat(query(mainBranch)).skippingTypesCheck().matches("VALUES '%s'".formatted(id(newCommit)));

            assertUpdate("CALL git_refreshed.system.refresh('default')");
            assertQueryFails("CALL git_refreshed.system.refresh('unknown')", "Unknown schema: unknown");
        }
    }

    @Test
    void testBranchesFollowUpstream(@TempDir Path cacheDirectory)
            throws Exception
    {
        try (TestingGitRepository upstream = new TestingGitRepository()) {
            getQueryRunner().createCatalog("git_upstream", "git", ImmutableMap.of(
                    "git.url", upstream.getUri().toString(),
                    "git.cache-directory", cacheDirectory.toString(),
                    "git.fetch-interval", "0s"));
            assertThat(query("SELECT count(*) FROM git_upstream.default.branches WHERE name LIKE '%/feature'"))
                    .matches("VALUES BIGINT '1'");

            RevCommit newCommit = upstream.addEmptyCommit("A commit pushed after the clone");
            upstream.deleteBranch(FEATURE_BRANCH);

            assertThat(query("SELECT count(*) FROM git_upstream.default.branches WHERE name LIKE '%/feature'"))
                    .matches("VALUES BIGINT '0'");
            assertThat(query("SELECT DISTINCT object_id FROM git_upstream.default.branches WHERE name LIKE '%/main'"))
                    .skippingTypesCheck()
                    .matches("VALUES '%s'".formatted(id(newCommit)));
        }
    }

    @Test
    void testTags()
    {
        assertThat(query("SELECT name, object_id, tag_time FROM tags"))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            ('refs/tags/v0.1', '%s', NULL),
                            ('refs/tags/v1.0', '%s', TIMESTAMP '2024-01-01 21:30:00 +05:30')
                        """.formatted(id(repository.getInitialCommit()), repository.getAnnotatedTagId().name()));
    }

    @Test
    void testTagsCommitId()
    {
        assertThat(query("SELECT name, commit_id FROM tags"))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            ('refs/tags/v0.1', '%s'),
                            ('refs/tags/v1.0', '%s')
                        """.formatted(id(repository.getInitialCommit()), id(repository.getMergeCommit())));
    }

    @Test
    void testTreesForCommit()
            throws IOException
    {
        RevCommit commit = repository.getEmptyCommit();
        assertThat(query("SELECT commit_id, file_mode, mode, object_id, file_name, path_name, depth FROM trees WHERE commit_id = '%s'".formatted(id(commit))))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            ('%1$s', 'file', '100644', '%2$s', 'README.md', 'README.md', 0),
                            ('%1$s', 'directory', '040000', '%7$s', 'docs', 'docs', 0),
                            ('%1$s', 'file', '100644', '%3$s', 'guide.md', 'docs/guide.md', 1),
                            ('%1$s', 'file', '100644', '%4$s', 'feature.txt', 'feature.txt', 0),
                            ('%1$s', 'file', '100644', '%5$s', 'image.bin', 'image.bin', 0),
                            ('%1$s', 'file', '100644', '%6$s', 'new-name.txt', 'new-name.txt', 0)
                        """.formatted(
                                id(commit),
                                blobId(commit, README),
                                blobId(commit, GUIDE),
                                blobId(commit, FEATURE_FILE),
                                blobId(commit, BINARY),
                                blobId(commit, RENAMED_NEW),
                                blobId(commit, "docs")));
    }

    @Test
    void testTreesForAllCommits()
    {
        assertThat(query("SELECT commit_id, count(*) FROM trees GROUP BY commit_id"))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            ('%s', BIGINT '5'),
                            ('%s', BIGINT '5'),
                            ('%s', BIGINT '6'),
                            ('%s', BIGINT '6'),
                            ('%s', BIGINT '5'),
                            ('%s', BIGINT '6'),
                            ('%s', BIGINT '6')
                        """.formatted(
                                id(repository.getInitialCommit()),
                                id(repository.getUpdateCommit()),
                                id(repository.getFeatureCommit()),
                                id(repository.getRenameCommit()),
                                id(repository.getDeleteCommit()),
                                id(repository.getMergeCommit()),
                                id(repository.getEmptyCommit())));
    }

    @Test
    void testTreesFilterByCommitIdAndObjectId()
            throws IOException
    {
        assertThat(query("SELECT commit_id, path_name FROM trees WHERE commit_id IN ('%s', '%s') AND object_id = '%s'".formatted(
                id(repository.getInitialCommit()),
                id(repository.getEmptyCommit()),
                blobId(repository.getInitialCommit(), README))))
                .skippingTypesCheck()
                .matches("VALUES ('%s', 'README.md')".formatted(id(repository.getInitialCommit())));
    }

    @Test
    void testTreesFilterByObjectId()
            throws IOException
    {
        assertThat(query("SELECT commit_id FROM trees WHERE object_id = '%s'".formatted(blobId(repository.getInitialCommit(), README))))
                .skippingTypesCheck()
                .matches("VALUES '%s'".formatted(id(repository.getInitialCommit())));
    }

    @Test
    void testObjects()
            throws IOException
    {
        assertThat(query("SELECT object_id FROM objects"))
                .skippingTypesCheck()
                .matches("VALUES '%s', '%s', '%s', '%s', '%s', '%s', '%s'".formatted(
                        blobId(repository.getInitialCommit(), README),
                        blobId(repository.getUpdateCommit(), README),
                        blobId(repository.getInitialCommit(), GUIDE),
                        blobId(repository.getInitialCommit(), RENAMED_OLD),
                        blobId(repository.getInitialCommit(), DELETED),
                        blobId(repository.getFeatureCommit(), FEATURE_FILE),
                        blobId(repository.getRenameCommit(), BINARY)));
        assertThat(query("SELECT contents FROM objects WHERE object_id = '%s'".formatted(blobId(repository.getRenameCommit(), BINARY))))
                .matches("VALUES X'89504e4700000000fffe0001'");
    }

    @Test
    void testDiffStatsModifiedFile()
    {
        assertThat(query("SELECT commit_id, old_commit_id, path_name, old_path_name, change_type, added_lines, deleted_lines FROM diff_stats WHERE commit_id = '%s'".formatted(
                id(repository.getUpdateCommit()))))
                .skippingTypesCheck()
                .matches("VALUES ('%s', '%s', 'README.md', 'README.md', 'modified', 3, 0)".formatted(
                        id(repository.getUpdateCommit()),
                        id(repository.getInitialCommit())));
    }

    @Test
    void testDiffStatsJoinObjects()
            throws IOException
    {
        assertThat(query("SELECT d.path_name, o.object_id FROM diff_stats d JOIN objects o ON d.object_id = o.object_id WHERE d.commit_id = '%s'".formatted(
                id(repository.getUpdateCommit()))))
                .skippingTypesCheck()
                .matches("VALUES ('README.md', '%s')".formatted(blobId(repository.getUpdateCommit(), README)));
    }

    @Test
    void testDiffStatsRootCommit()
    {
        assertThat(query("SELECT old_commit_id, path_name, change_type, added_lines, deleted_lines FROM diff_stats WHERE commit_id = '%s'".formatted(
                id(repository.getInitialCommit()))))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            (NULL, 'README.md', 'added', 1, 0),
                            (NULL, 'docs/guide.md', 'added', 3, 0),
                            (NULL, 'old-name.txt', 'added', 3, 0),
                            (NULL, 'to-delete.txt', 'added', 1, 0)
                        """);
    }

    @Test
    void testDiffStatsMergeCommitComparesWithFirstParent()
    {
        assertThat(query("SELECT old_commit_id, path_name, change_type, added_lines, deleted_lines FROM diff_stats WHERE commit_id = '%s'".formatted(
                id(repository.getMergeCommit()))))
                .skippingTypesCheck()
                .matches("VALUES ('%s', 'feature.txt', 'added', 1, 0)".formatted(id(repository.getDeleteCommit())));
    }

    @Test
    void testDiffStatsComparesWithParent()
    {
        assertThat(query("SELECT old_commit_id, path_name, change_type FROM diff_stats WHERE commit_id = '%s'".formatted(
                id(repository.getRenameCommit()))))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            ('%1$s', 'new-name.txt', 'renamed'),
                            ('%1$s', 'image.bin', 'added')
                        """.formatted(id(repository.getUpdateCommit())));
    }

    @Test
    void testDiffStatsAddedFile()
            throws IOException
    {
        assertThat(query("SELECT object_id, path_name, old_path_name, change_type FROM diff_stats WHERE commit_id = '%s'".formatted(
                id(repository.getFeatureCommit()))))
                .skippingTypesCheck()
                .matches("VALUES ('%s', 'feature.txt', NULL, 'added')".formatted(blobId(repository.getFeatureCommit(), FEATURE_FILE)));
    }

    @Test
    void testDiffStatsBinaryFile()
    {
        assertThat(query("SELECT change_type, added_lines, deleted_lines FROM diff_stats WHERE path_name = 'image.bin'"))
                .skippingTypesCheck()
                .matches("VALUES ('added', CAST(NULL AS integer), CAST(NULL AS integer))");
    }

    @Test
    void testDiffStatsDeletedFile()
            throws IOException
    {
        assertThat(query("SELECT object_id, old_object_id, path_name, old_path_name, change_type FROM diff_stats WHERE commit_id = '%s'".formatted(
                id(repository.getDeleteCommit()))))
                .skippingTypesCheck()
                .matches("VALUES (NULL, '%s', NULL, 'to-delete.txt', 'deleted')".formatted(blobId(repository.getInitialCommit(), DELETED)));
    }

    @Test
    void testIdents()
    {
        assertThat(query("SELECT email, name, extra_emails, extra_names FROM idents"))
                .skippingTypesCheck()
                .matches(
                        """
                        VALUES
                            ('alice@example.com', 'Alice', CAST(ARRAY[] AS array(varchar)), ARRAY['Alice Example', 'alice']),
                            ('bob@example.com', 'Bob Builder', CAST(ARRAY[] AS array(varchar)), CAST(ARRAY[] AS array(varchar))),
                            ('carol@example.com', 'Carol', CAST(ARRAY[] AS array(varchar)), CAST(ARRAY[] AS array(varchar)))
                        """);
    }

    @Test
    void testCommitStatsSimilarityOfRenamesOnly()
    {
        assertThat(query("SELECT similarity_score, array_sort(change_types) FROM commit_stats WHERE object_id = '%s'".formatted(id(repository.getRenameCommit()))))
                .skippingTypesCheck()
                .matches("VALUES (DOUBLE '100', ARRAY['added', 'renamed'])");
        assertThat(query("SELECT similarity_score, change_types FROM commit_stats WHERE object_id = '%s'".formatted(id(repository.getUpdateCommit()))))
                .skippingTypesCheck()
                .matches("VALUES (CAST(NULL AS double), ARRAY['modified'])");
    }

    @Test
    void testCommitStatsIncludesAllCommits()
    {
        assertThat(query("SELECT count(*) FROM commit_stats"))
                .matches("VALUES BIGINT '8'");
        assertThat(query("SELECT changed_files FROM commit_stats WHERE object_id = '%s'".formatted(id(repository.getEmptyCommit()))))
                .matches("VALUES BIGINT '0'");
    }

    private String blobId(RevCommit commit, String path)
            throws IOException
    {
        return repository.getBlobId(commit, path).name();
    }

    private static String id(RevCommit commit)
    {
        return commit.name();
    }

    private static String treeId(RevCommit commit)
    {
        return commit.getTree().name();
    }
}
