-- Create enum types
CREATE TYPE job_status AS ENUM (
    'planned',
    'initiated',
    'queued',
    'running',
    'completed',
    'failed',
    'cancelled'
);

CREATE TYPE analysis_status AS ENUM (
    'pending',
    'running',
    'completed',
    'failed',
    'cancelled'
);

CREATE TYPE recovery_type AS ENUM (
    'startup',
    'manual',
    'scheduled',
    'crash'
);

-- Analysis table
CREATE TABLE IF NOT EXISTS irp_analysis (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    status analysis_status DEFAULT 'pending',
    yaml_config JSONB,
    created_ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_ts TIMESTAMP WITH TIME ZONE
);

-- Configuration table
CREATE TABLE IF NOT EXISTS irp_configuration (
    id SERIAL PRIMARY KEY,
    analysis_id INTEGER NOT NULL REFERENCES irp_analysis(id) ON DELETE CASCADE,
    config_name VARCHAR(255) NOT NULL,
    config_data JSONB NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    version INTEGER DEFAULT 1,
    CONSTRAINT unique_active_config_per_analysis UNIQUE (analysis_id, config_name, version)
);

-- Job table
CREATE TABLE IF NOT EXISTS irp_job (
    id SERIAL PRIMARY KEY,
    analysis_id INTEGER NOT NULL REFERENCES irp_analysis(id) ON DELETE CASCADE,
    configuration_id INTEGER NOT NULL REFERENCES irp_configuration(id),
    workflow_id VARCHAR(255),
    status job_status DEFAULT 'planned',
    retry_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    initiation_ts TIMESTAMP WITH TIME ZONE,
    updated_ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_ts TIMESTAMP WITH TIME ZONE,
    parent_job_id INTEGER REFERENCES irp_job(id),
    celery_task_id VARCHAR(255),
    last_poll_ts TIMESTAMP WITH TIME ZONE,
    CONSTRAINT unique_workflow_id UNIQUE (workflow_id)
);

-- Workflow status tracking
CREATE TABLE IF NOT EXISTS irp_workflow_status (
    id SERIAL PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES irp_job(id) ON DELETE CASCADE,
    status VARCHAR(50) NOT NULL,
    response_data JSONB,
    http_status_code INTEGER,
    polled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    poll_duration_ms INTEGER
);

-- Retry history
CREATE TABLE IF NOT EXISTS irp_retry_history (
    id SERIAL PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES irp_job(id) ON DELETE CASCADE,
    retry_attempt INTEGER NOT NULL,
    error_code VARCHAR(20),
    error_message TEXT,
    retry_after_seconds INTEGER,
    attempted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    succeeded BOOLEAN DEFAULT false
);

-- System recovery tracking
CREATE TABLE IF NOT EXISTS irp_system_recovery (
    id SERIAL PRIMARY KEY,
    recovery_type recovery_type NOT NULL,
    jobs_recovered INTEGER DEFAULT 0,
    jobs_resubmitted INTEGER DEFAULT 0,
    jobs_resumed_polling INTEGER DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    recovery_metadata JSONB
);

-- Celery task state (for recovery)
CREATE TABLE IF NOT EXISTS irp_celery_task_state (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(255) UNIQUE NOT NULL,
    task_name VARCHAR(255) NOT NULL,
    job_id INTEGER REFERENCES irp_job(id),
    task_args JSONB,
    task_kwargs JSONB,
    eta TIMESTAMP WITH TIME ZONE,
    expires TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create update timestamp trigger function
CREATE OR REPLACE FUNCTION update_updated_ts_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_ts = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';