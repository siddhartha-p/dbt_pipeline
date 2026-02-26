-- API Users table for authentication and authorization
CREATE SCHEMA IF NOT EXISTS api;

CREATE TABLE IF NOT EXISTS api.users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    role VARCHAR(20) DEFAULT 'user' CHECK (role IN ('admin', 'user')),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_users_username ON api.users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON api.users(email);

-- Insert initial admin user (password: admin123 - should be changed in production)
-- Password hash generated with bcrypt for 'admin123'
INSERT INTO api.users (username, email, hashed_password, role)
VALUES (
    'admin',
    'admin@company.com',
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/X4.G0bTlBVQqj0zKm',
    'admin'
)
ON CONFLICT (username) DO NOTHING;
