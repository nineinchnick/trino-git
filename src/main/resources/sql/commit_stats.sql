SELECT
    c.object_id,
    c.author_name,
    c.author_email,
    c.committer_name,
    c.committer_email,
    c.message,
    c.parents,
    c.tree_id,
    c.commit_time,
    c.author_time,
    coalesce(sum(s.added_lines), 0) AS added_lines,
    coalesce(sum(s.deleted_lines), 0) AS deleted_lines,
    count(s.commit_id) AS changed_files,
    avg(s.similarity_score) FILTER (WHERE s.change_type IN ('renamed', 'copied')) AS similarity_score,
    coalesce(array_agg(s.change_type) FILTER (WHERE s.change_type IS NOT NULL), CAST(ARRAY[] AS array(varchar))) AS change_types
FROM
    commits c
LEFT JOIN diff_stats s ON
    s.commit_id = c.object_id
GROUP BY
    c.object_id,
    c.author_email,
    c.author_name,
    c.committer_email,
    c.committer_name,
    c.message,
    c.parents,
    c.tree_id,
    c.commit_time,
    c.author_time