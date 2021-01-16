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
    sum(s.added_lines) AS added_lines,
    sum(s.deleted_lines) AS deleted_lines,
    count(s.commit_id) AS changed_files,
    avg(s.similarity_score) AS similarity_score,
    array_agg(s.change_type) AS change_types
FROM
    commits c
JOIN diff_stats s ON
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
    c.commit_time