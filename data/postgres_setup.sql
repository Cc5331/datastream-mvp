CREATE TABLE IF NOT EXISTS students (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    college TEXT,
    gpa NUMERIC(3, 2),
    enrolled_at TIMESTAMP
);

INSERT INTO students (id, name, college, gpa, enrolled_at) VALUES
    (1, '张三', '计算机学院', 3.85, '2026-09-01 08:30:00'),
    (2, '李四', '软件学院', 3.62, '2026-09-02 09:00:00'),
    (3, 'Alice', NULL, 3.91, '2026-09-03 10:15:00')
ON CONFLICT (id) DO NOTHING;
