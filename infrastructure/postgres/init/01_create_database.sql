-- Create database if not exists (handled by Docker env vars)
-- This file is for any database-level settings

-- Set timezone
SET timezone = 'UTC';

-- Create extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For text search

-- Set default settings
ALTER DATABASE irp_db SET log_statement = 'all';
ALTER DATABASE irp_db SET log_duration = on;