"""
Command-line interface for recovery management.

Usage:
    docker exec -it irp-app python src/recovery_cli.py --help
    docker exec -it irp-app python src/recovery_cli.py status
    docker exec -it irp-app python src/recovery_cli.py trigger
    docker exec -it irp-app python src/recovery_cli.py health
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta
from tabulate import tabulate

sys.path.append(str(Path(__file__).parent.parent))

from src.services.recovery_service import RecoveryService
from src.db.session import get_db_session
from src.db.models import Job, Batch, SystemRecovery
import src.core.constants as constants


def format_duration(seconds):
    """Format duration in seconds to human-readable format."""
    if seconds is None:
        return "N/A"
    
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


def show_status(args):
    """Show current system and recovery status."""
    service = RecoveryService()
    
    print("\n" + "=" * 80)
    print("SYSTEM RECOVERY STATUS")
    print("=" * 80)
    
    # Get system health
    health = service.check_system_health()
    
    print(f"\nSystem Status: {health['status'].upper()}")
    print(f"Timestamp: {health['timestamp']}")
    
    if health['issues']:
        print("\n⚠ Issues Detected:")
        for issue in health['issues']:
            print(f"  - {issue}")
    
    if health['recommendations']:
        print("\n💡 Recommendations:")
        for rec in health['recommendations']:
            print(f"  - {rec}")
    
    # Show metrics
    print("\n📊 System Metrics:")
    metrics_table = []
    for key, value in health['metrics'].items():
        metrics_table.append([key.replace('_', ' ').title(), value])
    print(tabulate(metrics_table, headers=["Metric", "Value"], tablefmt="grid"))
    
    # Show last recovery
    if health.get('last_recovery'):
        print(f"\n🔄 Last Recovery:")
        print(f"  Type: {health['last_recovery']['type']}")
        print(f"  Time: {health['last_recovery']['timestamp']}")
        print(f"  Success: {'✓' if health['last_recovery']['success'] else '✗'}")
    
    # Show active jobs
    with get_db_session() as db:
        active_jobs = db.query(Job).filter(
            Job.status.in_(['submitted', 'queued', 'running'])
        ).count()
        
        pending_jobs = db.query(Job).filter(
            Job.status == 'pending'
        ).count()
        
        active_batches = db.query(Batch).filter(
            Batch.status.in_(['pending', 'running'])
        ).count()
        
        print(f"\n📈 Current Activity:")
        print(f"  Active Jobs: {active_jobs}")
        print(f"  Pending Jobs: {pending_jobs}")
        print(f"  Active Batches: {active_batches}")
    
    # Show problem jobs if any
    if 'stale_job_ids' in health and health['stale_job_ids']:
        print(f"\n⚠ Stale Jobs (first 10): {health['stale_job_ids']}")
    
    if 'stuck_batch_ids' in health and health['stuck_batch_ids']:
        print(f"⚠ Stuck Batches: {health['stuck_batch_ids']}")
    
    print()


def trigger_recovery(args):
    """Trigger manual recovery."""
    service = RecoveryService()
    
    print("\n🚀 Triggering manual recovery...")
    result = service.trigger_manual_recovery()
    
    print(f"✓ Recovery started")
    print(f"  Task ID: {result['task_id']}")
    print(f"  Timestamp: {result['timestamp']}")
    print("\nMonitor progress with: recovery_cli.py monitor")
    print()


def show_health(args):
    """Show detailed health analysis."""
    service = RecoveryService()
    
    print("\n" + "=" * 80)
    print("SYSTEM HEALTH ANALYSIS")
    print("=" * 80)
    
    # Get recovery statistics
    stats_24h = service.get_recovery_statistics(24)
    stats_7d = service.get_recovery_statistics(24 * 7)
    
    print("\n📊 Recovery Statistics (Last 24 Hours):")
    print(f"  Total Recoveries: {stats_24h['recovery_count']}")
    print(f"  Jobs Recovered: {stats_24h['total_jobs_recovered']}")
    print(f"  Jobs Resubmitted: {stats_24h['total_jobs_resubmitted']}")
    print(f"  Jobs Resumed: {stats_24h['total_jobs_resumed_polling']}")
    if stats_24h['average_duration_seconds']:
        print(f"  Avg Duration: {format_duration(stats_24h['average_duration_seconds'])}")
    if stats_24h['success_rate'] is not None:
        print(f"  Success Rate: {stats_24h['success_rate']}%")
    
    print("\n📊 Recovery Statistics (Last 7 Days):")
    print(f"  Total Recoveries: {stats_7d['recovery_count']}")
    print(f"  Jobs Recovered: {stats_7d['total_jobs_recovered']}")
    print(f"  Jobs Resubmitted: {stats_7d['total_jobs_resubmitted']}")
    
    # Show recovery types breakdown
    if stats_24h['recovery_types']:
        print("\n🔍 Recovery Types (Last 24h):")
        for rtype, count in stats_24h['recovery_types'].items():
            print(f"  {rtype}: {count}")
    
    # Get recent recovery history
    history = service.get_recovery_history(5)
    
    if history:
        print("\n📜 Recent Recovery History:")
        table_data = []
        for rec in history:
            table_data.append([
                rec['recovery_id'],
                rec['type'],
                rec['started_at'][:19],  # Trim microseconds
                format_duration(rec['duration_seconds']),
                f"{rec['jobs_resubmitted']}",
                f"{rec['jobs_resumed_polling']}",
                "✓" if rec['success'] else "✗"
            ])
        
        print(tabulate(
            table_data,
            headers=["ID", "Type", "Started", "Duration", "Resubmit", "Resume", "Success"],
            tablefmt="grid"
        ))
    
    print()


def monitor_recovery(args):
    """Monitor ongoing recovery."""
    service = RecoveryService()
    
    print("\n" + "=" * 80)
    print("RECOVERY MONITOR")
    print("=" * 80)
    
    # Get latest recovery status
    status = service.get_recovery_status()
    
    if status['status'] == 'no_recovery_found':
        print("\n❌ No recovery operations found")
        return
    
    print(f"\n🔄 Recovery #{status['recovery_id']}")
    print(f"  Type: {status['type']}")
    print(f"  Status: {status['status'].upper()}")
    print(f"  Started: {status['started_at']}")
    
    if status['completed_at']:
        print(f"  Completed: {status['completed_at']}")
        print(f"  Duration: {format_duration(status['duration_seconds'])}")
    else:
        # Calculate elapsed time
        from datetime import datetime, timezone
        start = datetime.fromisoformat(status['started_at'].replace('+00:00', '')).replace(tzinfo=timezone.utc)
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        print(f"  Elapsed: {format_duration(elapsed)}")
    
    # Show metrics
    if status.get('detailed_metrics'):
        metrics = status['detailed_metrics']
        print("\n📊 Recovery Metrics:")
        print(f"  Pending Submitted: {metrics['pending_submitted']}")
        print(f"  Polling Resumed: {metrics['polling_resumed']}")
        print(f"  Resubmitted: {metrics['resubmitted']}")
        print(f"  Status Updated: {metrics['status_updated']}")
        print(f"  Batches Checked: {metrics['batch_checked']}")
        if metrics['errors'] > 0:
            print(f"  ⚠ Errors: {metrics['errors']}")
    
    # Show job details if available
    if status.get('job_details'):
        details = status['job_details']
        if details.get('pending_jobs'):
            print(f"\n  Pending Jobs Submitted: {len(details['pending_jobs'])}")
            if len(details['pending_jobs']) <= 10:
                print(f"    {details['pending_jobs']}")
        
        if details.get('active_jobs'):
            print(f"  Active Jobs Resumed: {len(details['active_jobs'])}")
            if len(details['active_jobs']) <= 10:
                print(f"    {details['active_jobs']}")
        
        if details.get('resubmitted_jobs'):
            print(f"  Jobs Resubmitted: {len(details['resubmitted_jobs'])}")
            if len(details['resubmitted_jobs']) <= 10:
                print(f"    {details['resubmitted_jobs']}")
    
    if status['status'] == 'in_progress':
        print("\n⏳ Recovery in progress... Check back for updates.")
    else:
        print("\n✓ Recovery complete")
    
    print()


def fix_issues(args):
    """Attempt to fix common issues."""
    print("\n" + "=" * 80)
    print("AUTOMATIC ISSUE RESOLUTION")
    print("=" * 80)
    
    fixed = []
    
    with get_db_session() as db:
        # Fix jobs without workflow IDs
        orphan_active = db.query(Job).filter(
            Job.status.in_(['submitted', 'queued', 'running']),
            Job.workflow_id == None
        ).all()
        
        if orphan_active:
            print(f"\n🔧 Found {len(orphan_active)} active jobs without workflow IDs")
            for job in orphan_active:
                job.status = 'pending'
                job.initiation_ts = None
                fixed.append(f"Reset job {job.id} to pending")
            db.commit()
            print(f"  ✓ Reset {len(orphan_active)} jobs to pending status")
        
        # Fix old pending jobs
        from datetime import datetime, timezone, timedelta
        old_threshold = datetime.now(timezone.utc) - timedelta(hours=24)
        old_pending = db.query(Job).filter(
            Job.status == 'pending',
            Job.created_ts < old_threshold
        ).count()
        
        if old_pending > 0:
            print(f"\n⚠ Found {old_pending} pending jobs older than 24 hours")
            print("  Run 'recovery_cli.py trigger' to submit them")
    
    if fixed:
        print(f"\n✓ Fixed {len(fixed)} issues:")
        for fix in fixed[:10]:  # Show first 10
            print(f"  - {fix}")
        
        print("\n💡 Trigger recovery to process fixed jobs: recovery_cli.py trigger")
    else:
        print("\n✓ No automatic fixes needed")
    
    print()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="IRP Recovery Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  recovery_cli.py status      - Show current system status
  recovery_cli.py trigger     - Trigger manual recovery
  recovery_cli.py health      - Show detailed health analysis  
  recovery_cli.py monitor     - Monitor ongoing recovery
  recovery_cli.py fix         - Attempt to fix common issues
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show system status')
    status_parser.set_defaults(func=show_status)
    
    # Trigger command
    trigger_parser = subparsers.add_parser('trigger', help='Trigger manual recovery')
    trigger_parser.set_defaults(func=trigger_recovery)
    
    # Health command
    health_parser = subparsers.add_parser('health', help='Show health analysis')
    health_parser.set_defaults(func=show_health)
    
    # Monitor command
    monitor_parser = subparsers.add_parser('monitor', help='Monitor recovery')
    monitor_parser.set_defaults(func=monitor_recovery)
    
    # Fix command
    fix_parser = subparsers.add_parser('fix', help='Fix common issues')
    fix_parser.set_defaults(func=fix_issues)
    
    args = parser.parse_args()
    
    if not args.command:
        # Default to status
        show_status(args)
    else:
        args.func(args)


if __name__ == "__main__":
    main()