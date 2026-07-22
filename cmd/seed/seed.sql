-- The plaintext token lives in collector/dev/querysheriff-collector.yml (qsc_402MUek5w0PjR8fggjJdaDO2ug7)
INSERT INTO collector_tokens (server_name, token_hash)
VALUES ('dev-postgres-17', 'a88509ca30de8ad7b7f9bf853235ebadfad47f045dea723f13d0fe666a3c6fa3')
ON CONFLICT DO NOTHING;

-- The super admin: admin@example.com / password "123123".
INSERT INTO users (name, email, password_hash, is_super_admin)
VALUES ('admin', 'admin@example.com', '$2a$10$4kZNNT2PVyU9fy3d.bHeRu4wPBUzg1tlF29hgKAnIbrGuMZTGaJNK', true)
ON CONFLICT DO NOTHING;
