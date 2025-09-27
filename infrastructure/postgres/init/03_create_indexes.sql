-- Performance indexes
CREATE INDEX idx_job_status ON irp_job(status);
CREATE INDEX idx_job_analysis_id ON irp_job(analysis_id);
CREATE INDEX idx_job_workflow_id ON irp_job(workflow_id);
CREATE INDEX idx_job_updated_ts ON irp_job(updated_ts);
CREATE INDEX idx_job_incomplete ON irp_job(status) 
    WHERE status IN ('initiated', 'queued', 'running');

CREATE INDEX idx_analysis_status ON irp_analysis(status);
CREATE INDEX idx_analysis_updated_ts ON irp_analysis(updated_ts);

CREATE INDEX idx_workflow_status_job_id ON irp_workflow_status(job_id);
CREATE INDEX idx_workflow_status_polled_at ON irp_workflow_status(polled_at);

CREATE INDEX idx_config_analysis_id ON irp_configuration(analysis_id);
CREATE INDEX idx_config_active ON irp_configuration(is_active);

CREATE INDEX idx_retry_history_job_id ON irp_retry_history(job_id);
CREATE INDEX idx_celery_task_job_id ON irp_celery_task_state(job_id);

-- Create triggers for updated_ts
CREATE TRIGGER update_analysis_updated_ts 
    BEFORE UPDATE ON irp_analysis 
    FOR EACH ROW EXECUTE FUNCTION update_updated_ts_column();

CREATE TRIGGER update_configuration_updated_ts 
    BEFORE UPDATE ON irp_configuration 
    FOR EACH ROW EXECUTE FUNCTION update_updated_ts_column();

CREATE TRIGGER update_job_updated_ts 
    BEFORE UPDATE ON irp_job 
    FOR EACH ROW EXECUTE FUNCTION update_updated_ts_column();

-- Create composite indexes for common queries
CREATE INDEX idx_job_status_analysis ON irp_job(analysis_id, status);
CREATE INDEX idx_job_recovery_check ON irp_job(status, updated_ts)
    WHERE status IN ('initiated', 'queued', 'running');