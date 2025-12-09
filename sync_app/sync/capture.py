import frappe
import json
import uuid
from frappe.utils import now


def get_device_id():
    """
    MASTER: Always returns "MASTER"
    CHILD:  Returns unique ID like "A1B2C3D4"
    
    This identifies which offline instance made the change.
    Reads from Sync Configuration's custom_device_id field.
    """
    
    # Get device ID from Sync Configuration
    device_id = frappe.db.get_value("Sync Configuration", None, "custom_device_id")
    
    if not device_id:
        # If not set, check if this is master or child
        is_master = frappe.db.get_value("Sync Configuration", None, "is_master")
        
        if is_master:
            device_id = "MASTER"
        else:
            # Generate a new device ID
            device_id = str(uuid.uuid4())[:8].upper()
        
        # Save it to Sync Configuration
        try:
            config = frappe.get_doc("Sync Configuration")
            config.custom_device_id = device_id
            config.save(ignore_permissions=True)
            frappe.db.commit()
        except Exception as e:
            frappe.log_error(f"Failed to save device_id: {str(e)}", "Sync Device ID Error")
    
    return device_id



def capture_change(doc, method=None):
    """
    BOTH MASTER AND OFFLINE
    Capture every document change into transaction log
    
    This runs automatically when:
    - after_insert: Document is created
    - after_save: Document is updated
    - after_submit: Document is submitted
    - after_amend: Document is amended
    - after_cancel: Document is cancelled
    - before_delete: Document is deleted
    """
    
    # Skip if in special modes
    if frappe.flags.in_rollback or frappe.flags.in_patch or frappe.flags.sync_in_progress:
        return
    
    # Import excluded list
    from sync_app.hooks import SYNC_EXCLUDED_DOCTYPES
    
    # Skip excluded doctypes
    if doc.doctype in SYNC_EXCLUDED_DOCTYPES:
        return
    
    # Determine what operation happened
    operation = _determine_operation(doc, method)
    
    try:
        # Create transaction log entry
        log = frappe.new_doc("Sync Transaction Log")
        log.timestamp = now()
        log.doctype_name = doc.doctype
        log.document_name = doc.name
        log.operation = operation
        log.doc_data = json.dumps(doc.as_dict(), default=str)
        log.synced = 0
        log.sync_status = "pending"
        
        # Check if this change came from a sync operation (via header)
        source_device_id = None
        try:
            if frappe.request and hasattr(frappe.request, 'headers'):
                source_device_id = frappe.request.headers.get('X-Source-Device-ID')
        except Exception:
            pass
            
        log.device_id = source_device_id or get_device_id()
        log.server_version = frappe.get_value("System Settings", None, "app_version") or "unknown"
        log.sync_attempt_count = 0
        
        # Set flag to prevent recursive captures
        frappe.flags.sync_in_progress = True
        log.insert(ignore_permissions=True)
        frappe.flags.sync_in_progress = False
        
    except Exception as e:
        frappe.log_error(f"Failed to capture sync transaction: {str(e)}", "Sync Capture Error")


def _determine_operation(doc, method):
    """
    Determine what operation this is based on the hook method name
    """
    if not method:
        return "update"
    
    # Remove hook prefix
    method = method.replace("after_", "").replace("before_", "")
    
    # Map to operation names
    operation_map = {
        "insert": "create",
        "save": "update",
        "submit": "submit",
        "amend": "amend",
        "cancel": "cancel",
        "delete": "delete",
    }
    
    return operation_map.get(method, "update")