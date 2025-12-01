
import frappe


@frappe.whitelist()
def sync_up_to_master(batch_size=50):
    """
    OFFLINE ONLY
    Endpoint: Push offline changes to master
    Called from: UI button "Sync Up"
    """
    try:
        from sync_app.sync.engine import OfflineSyncEngine
        engine = OfflineSyncEngine()
        results = engine.sync_up(batch_size=batch_size)
        
        # Ensure results always has a status field
        if not isinstance(results, dict):
            results = {"status": "success", "message": str(results)}
        elif "status" not in results:
            results["status"] = "success"
            
        return results
    except Exception as e:
        frappe.log_error(str(e), "Sync Up Failed")
        return {"status": "error", "message": str(e)}



@frappe.whitelist()
def sync_down_from_master(batch_size=50):
    """
    OFFLINE ONLY
    Endpoint: Pull changes from master created by other offline clients
    Called from: UI button "Sync Down"
    """
    try:
        from sync_app.sync.engine import OfflineSyncEngine
        engine = OfflineSyncEngine()
        results = engine.sync_down(batch_size=batch_size)
        
        # Ensure results always has a status field
        if not isinstance(results, dict):
            results = {"status": "success", "message": str(results)}
        elif "status" not in results:
            results["status"] = "success"
            
        return results
    except Exception as e:
        frappe.log_error(str(e), "Sync Down Failed")
        return {"status": "error", "message": str(e)}


@frappe.whitelist()
def sync_bidirectional():
    """
    OFFLINE ONLY
    Do both sync up and sync down in one call
    """
    up_result = sync_up_to_master()
    down_result = sync_down_from_master()
    
    return {
        "status": "success",
        "sync_up": up_result,
        "sync_down": down_result
    }


@frappe.whitelist()
def get_sync_status():
    """
    BOTH MASTER and OFFLINE
    Get status of sync logs
    """
    return {
        "pending": frappe.db.count("Sync Transaction Log", {"synced": 0, "sync_status": "pending"}),
        "failed": frappe.db.count("Sync Transaction Log", {"synced": 0, "sync_status": "failed"}),
        "synced": frappe.db.count("Sync Transaction Log", {"synced": 1}),
    }


@frappe.whitelist()
def get_pending_logs(limit=100):
    """
    BOTH MASTER and OFFLINE
    Get detailed view of pending transactions
    """
    return frappe.get_list(
        "Sync Transaction Log",
        filters={"synced": 0},
        fields=["name", "timestamp", "doctype_name", "document_name", 
                "remote_document_name", "operation", "sync_status", "error_message"],
        order_by="timestamp asc",
        limit_page_length=limit
    )


@frappe.whitelist()
def retry_failed_sync(limit=10):
    """
    OFFLINE ONLY
    Retry failed sync attempts
    """
    frappe.db.sql("""
        UPDATE `tabSync Transaction Log`
        SET sync_status = 'pending', sync_attempt_count = 0
        WHERE sync_status = 'failed' AND sync_attempt_count < 3
        LIMIT %s
    """, (limit,))
    
    return sync_up_to_master(batch_size=limit)