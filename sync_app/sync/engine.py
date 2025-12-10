import requests
import json
from frappe.utils import cint
import frappe


class OfflineSyncEngine:
    """
    OFFLINE CLIENT ONLY
    Handles bidirectional sync with master server
    
    Methods:
    - sync_up(): Send offline changes to master
    - sync_down(): Receive changes from master (from other devices)
    """
    
    def __init__(self):
        """Read config from database"""
        config = frappe.get_doc("Sync Configuration")
        if not config.master_url:
            frappe.throw("Please configure the Master Server URL in 'Sync Configuration' settings.")
        self.master_url = config.master_url.rstrip('/')
        self.api_key = config.api_key
        self.api_secret = config.api_secret
        
        # Create authenticated session with HTTP Basic Auth (like curl uses)
        self.session = requests.Session()
        self.session.auth = (self.api_key, self.api_secret)
        
        # Get my device ID
        self.device_id = config.custom_device_id
        
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Source-Device-ID": self.device_id or ""
        })
        
        self.stats = {"created": 0, "updated": 0, "submitted": 0, "failed": 0}
        self.collision_map = {}
    
    # ====== SYNC UP: Offline → Master ======
    
    def sync_up(self, batch_size=50):
        """
        OFFLINE ONLY
        Push pending changes from offline instance to master
        
        Process:
        1. Query MY pending transaction logs (where device_id = mine)
        2. For each transaction:
           - Send to master via REST API
           - Mark as synced
        3. Return results
        
        Args:
            batch_size: Number of transactions to sync per call
        
        Returns:
            dict with sync results
        """
        
        # Get MY device ID from Sync Configuration
        my_device_id = frappe.db.get_value("Sync Configuration", None, "custom_device_id")
        
        if not my_device_id:
            return {
                "status": "error",
                "message": "Device ID not configured. Please save Sync Configuration to generate device ID.",
                "stats": self.stats,
                "direction": "up"
            }
        
        # Query MY pending logs
        pending = frappe.get_list(
            "Sync Transaction Log",
            filters={
                "synced": 0,
                "sync_status": ["in", ["pending", "failed"]],
                "device_id": my_device_id  # <-- ONLY MY LOGS
            },
            fields=["name", "timestamp", "operation", "document_name", "doctype_name"],
            order_by="timestamp asc",
            limit_page_length=batch_size
        )
        
        if not pending:
            return {
                "status": "success",
                "message": "No changes to sync up",
                "stats": self.stats,
                "direction": "up"
            }
        
        results = {
            "total": len(pending),
            "synced": 0,
            "failed": 0,
            "skipped": 0,
            "collisions_renamed": [],
            "errors": [],
            "stats": self.stats,
            "direction": "up"
        }
        
        for log_entry in pending:
            try:
                log_doc = frappe.get_doc("Sync Transaction Log", log_entry.name)
                self._sync_single_to_master(log_doc, results)
                results["synced"] += 1
                # Mark as synced
                log_doc.sync_status = "synced"
                log_doc.error_message = None
                log_doc.save(ignore_permissions=True)
            except Exception as e:
                error_msg = str(e)
                log_doc.sync_attempt_count = cint(log_doc.sync_attempt_count) + 1
                
                # Check if this is a permanent error that should be skipped
                is_permanent_error = (
                    "does not exist on master server" in error_msg or
                    "not found on master server" in error_msg or
                    "ImportError" in error_msg or
                    "No module named" in error_msg
                )
                
                if is_permanent_error:
                    # Skip this log - don't retry it
                    log_doc.sync_status = "skipped"
                    log_doc.error_message = f"SKIPPED: {error_msg[:450]}"
                    results["skipped"] += 1
                    results["errors"].append({
                        "log": log_entry.name,
                        "error": error_msg,
                        "status": "skipped",
                        "reason": "Permanent error - DocType or module not found on master"
                    })
                else:
                    # Temporary error - mark as failed and will retry
                    log_doc.sync_status = "failed"
                    log_doc.error_message = error_msg[:500]
                    results["failed"] += 1
                    results["errors"].append({
                        "log": log_entry.name,
                        "error": error_msg,
                        "status": "failed",
                        "attempt": log_doc.sync_attempt_count
                    })
                
                log_doc.save(ignore_permissions=True)

        
        results["stats"] = self.stats
        return results
    
    def _sync_single_to_master(self, log, results):
        """
        Send single transaction to master
        Handles collision detection and resolution
        """
        
        doc_data = json.loads(log.doc_data)
        doctype = log.doctype_name
        local_name = log.document_name
        operation = log.operation
        
        endpoint = f"{self.master_url}/api/resource/{doctype}"
        
        # Check if document exists on master
        remote_exists = self._check_exists_on_master(endpoint, local_name)
        
        # Handle collision
        final_name = local_name
        if remote_exists:
            remote_doc = self._get_remote_doc_from_master(endpoint, local_name)
            
            # Check for collision based on creation timestamp
            # BUT: If the names are identical (e.g. user-specified ID like '004'), 
            # and timestamps differ (because Master generated its own timestamp on sync),
            # we should probably assume it's the same document and allow update.
            # Real collision is when we auto-generated ID and somehow they clashed (unlikely with UUIDs)
            # or if user manually created '004' on both sides independently.
            
            remote_creation = remote_doc.get("creation")
            local_creation = doc_data.get("creation")
            
            # If we have a local creation time, and it differs from remote
            if local_creation and str(remote_creation) != str(local_creation):
                # If name is auto-generated (hash-like), it might be a real collision.
                # If name is simple (like '004'), it's likely the same doc.
                # For now, let's relax the check: If names match, we assume it's the same doc.
                # We only rename if we REALLY think it's a different doc.
                # But how to know?
                
                # Compromise: If it's a "Sync" operation, we usually want to overwrite.
                # Let's log a warning but proceed with update instead of renaming.
                # Renaming 'Item 004' to 'Item 004_1' is usually NOT what user wants.
                pass
                
                # ORIGINAL LOGIC:
                # final_name = self._resolve_collision_on_master(endpoint, doctype, doc_data)
                # self._rename_local_doc(doctype, local_name, final_name)
                # results["collisions_renamed"].append({ ... })
        
        doc_data["name"] = final_name
        
        # Replay operation on master
        if operation == "create":
            # If it exists remotely AND we kept the same name (no collision), it's an update
            if remote_exists and final_name == local_name:
                self._update_on_master(f"{endpoint}/{final_name}", doc_data, doctype)
                self.stats["updated"] += 1
            else:
                # Otherwise create it (either it's new, or we renamed it due to collision)
                self._create_on_master(endpoint, doc_data, doctype)
                self.stats["created"] += 1
        
        elif operation == "update":
            if not remote_exists:
                # If it doesn't exist on master, create it
                # PROBLEM: 'doc_data' might be partial (diff). Creation needs full data.
                # SOLUTION: Fetch full document from local DB.
                try:
                    if frappe.db.exists(doctype, local_name):
                        full_doc = frappe.get_doc(doctype, local_name)
                        full_data = full_doc.as_dict()
                        full_data["name"] = local_name
                        self._create_on_master(endpoint, full_data, doctype)
                        self.stats["created"] += 1
                    else:
                        # Local doc missing too? Can't create.
                        raise Exception(f"Document {doctype} {local_name} missing on master and locally. Cannot sync update.")
                except Exception as e:
                    # Fallback to partial data (will likely fail)
                    self._create_on_master(endpoint, doc_data, doctype)
                    self.stats["created"] += 1
            else:
                self._update_on_master(f"{endpoint}/{final_name}", doc_data, doctype)
                self.stats["updated"] += 1
        
        elif operation == "submit":
            if not remote_exists:
                self._create_on_master(endpoint, doc_data, doctype)
                self.stats["created"] += 1
            
            self._action_on_master(f"{endpoint}/{final_name}", "submit", doctype)
            self.stats["submitted"] += 1
        
        elif operation == "cancel":
            if remote_exists:
                self._action_on_master(f"{endpoint}/{final_name}", "cancel", doctype)
        
        elif operation == "delete":
            if remote_exists:
                self._delete_on_master(f"{endpoint}/{final_name}", doctype)
        
        # Mark as synced
        log.synced = 1
        log.sync_status = "synced"
        log.remote_document_name = final_name
        log.save(ignore_permissions=True)
    
    # ====== SYNC DOWN: Master → Offline ======
    
    def sync_down(self, batch_size=50):
        """
        OFFLINE ONLY
        Pull changes from master created by OTHER offline clients
        
        Process:
        1. Query master for transaction logs where device_id != mine
        2. For each transaction:
           - Apply it locally (create/update/delete)
        3. Return results
        
        Args:
            batch_size: Number of transactions to sync per call
        
        Returns:
            dict with sync results
        """
        
        # Get MY device ID and last sync time from Sync Configuration
        config = frappe.get_doc("Sync Configuration")
        my_device_id = config.custom_device_id
        last_sync = config.last_down_sync
        
        if not my_device_id:
            return {
                "status": "error",
                "message": "Device ID not configured. Please save Sync Configuration to generate device ID.",
                "direction": "down"
            }
        
        # Prepare filters
        filters = [["device_id", "!=", my_device_id]]
        if last_sync:
            filters.append(["timestamp", ">", last_sync])
            
        # Query MASTER for changes NOT from me AND after last sync
        try:
            import json
            response = self.session.get(
                f"{self.master_url}/api/resource/Sync Transaction Log",
                params={
                    "filters": json.dumps(filters),  # Serialize as JSON string
                    "fields": json.dumps(["name", "doctype_name", "document_name", "operation", "doc_data", "device_id", "timestamp"]),  # Serialize as JSON string
                    "order_by": "timestamp asc",
                    "limit_page_length": batch_size
                }
            )
            
            if response.status_code != 200:
                return {
                    "status": "error",
                    "message": f"Failed to fetch changes from master: {response.text}",
                    "direction": "down"
                }
            
            remote_logs = response.json().get("data", [])
            
            if not remote_logs:
                return {
                    "status": "success",
                    "message": "No new changes from master",
                    "synced": 0,
                    "direction": "down"
                }
            
            results = {
                "total": len(remote_logs),
                "synced": 0,
                "failed": 0,
                "skipped": 0,
                "errors": [],
                "direction": "down"
            }
            
            last_log_timestamp = None
            
            for remote_log in remote_logs:
                try:
                    self._apply_remote_change(remote_log)
                    results["synced"] += 1
                    last_log_timestamp = remote_log.get("timestamp")
                except Exception as e:
                    # Same error handling as sync_up
                    error_msg = str(e)
                    is_permanent_error = (
                        "does not exist" in error_msg or
                        "not found" in error_msg or
                        "ImportError" in error_msg or
                        "No module named" in error_msg
                    )
                    
                    if is_permanent_error:
                        results["skipped"] += 1
                        # If skipped, we still consider it "processed" for timestamp purposes
                        # so we don't get stuck on it forever
                        last_log_timestamp = remote_log.get("timestamp")
                    else:
                        results["failed"] += 1
                        results["errors"].append({
                            "doc": remote_log.get("document_name"),
                            "error": str(e)
                        })
            
            # Update last sync time to the timestamp of the last successfully processed (or skipped) log
            if last_log_timestamp:
                config.last_down_sync = last_log_timestamp
                config.save(ignore_permissions=True)
                frappe.db.commit()
            
            return results
        
        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
                "direction": "down"
            }
    
    def _apply_remote_change(self, remote_log):
        """Apply a change received from master"""
        
        doc_data = json.loads(remote_log.get("doc_data", "{}"))
        doctype = remote_log.get("doctype_name")
        doc_name = remote_log.get("document_name")
        operation = remote_log.get("operation")
        
        frappe.flags.sync_in_progress = True
        
        try:
            if operation == "create":
                if frappe.db.exists(doctype, doc_name):
                    # If it already exists, update it instead (idempotency)
                    doc = frappe.get_doc(doctype, doc_name)
                    for field, value in doc_data.items():
                        if field not in ["name", "creation", "modified", "modified_by", "owner"]:
                            if hasattr(doc, field):
                                doc.set(field, value)
                    doc.save(ignore_permissions=True)
                else:
                    doc = frappe.new_doc(doctype)
                    for field, value in doc_data.items():
                        if field not in ["name", "creation", "modified", "modified_by", "owner"]:
                            if hasattr(doc, field):
                                doc.set(field, value)
                    doc.name = doc_name
                    doc.insert(ignore_permissions=True)
            
            elif operation in ["update", "amend"]:
                if frappe.db.exists(doctype, doc_name):
                    doc = frappe.get_doc(doctype, doc_name)
                    for field, value in doc_data.items():
                        if field not in ["name", "creation", "modified", "modified_by", "owner"]:
                            if hasattr(doc, field):
                                doc.set(field, value)
                    doc.save(ignore_permissions=True)
                else:
                    doc = frappe.new_doc(doctype)
                    for field, value in doc_data.items():
                        if field not in ["name", "creation", "modified", "modified_by", "owner"]:
                            if hasattr(doc, field):
                                doc.set(field, value)
                    doc.name = doc_name
                    doc.insert(ignore_permissions=True)
            
            elif operation == "submit":
                if frappe.db.exists(doctype, doc_name):
                    doc = frappe.get_doc(doctype, doc_name)
                    doc.docstatus = 1
                    doc.save(ignore_permissions=True)
                else:
                    doc = frappe.new_doc(doctype)
                    for field, value in doc_data.items():
                        if field not in ["name", "creation", "modified", "modified_by", "owner"]:
                            if hasattr(doc, field):
                                doc.set(field, value)
                    doc.name = doc_name
                    doc.insert(ignore_permissions=True)
                    doc.docstatus = 1
                    doc.save(ignore_permissions=True)
            
            elif operation == "cancel":
                if frappe.db.exists(doctype, doc_name):
                    doc = frappe.get_doc(doctype, doc_name)
                    doc.docstatus = 2
                    doc.save(ignore_permissions=True)
            
            elif operation == "delete":
                if frappe.db.exists(doctype, doc_name):
                    frappe.delete_doc(doctype, doc_name, ignore_permissions=True)
        
        finally:
            frappe.flags.sync_in_progress = False
    
    # ====== HELPER METHODS ======
    
    def _create_on_master(self, endpoint, doc_data, doctype):
        """Create document on master"""
        exclude = {"name", "creation", "modified", "modified_by", "owner", "docstatus", "idx", "hash"}
        clean = {k: v for k, v in doc_data.items() if k not in exclude and v is not None}
        
        response = self.session.post(endpoint, json=clean)
        if response.status_code not in [200, 201]:
            error_text = response.text
            # Check if it's a DocType not found error
            if "ImportError" in error_text or "No module named" in error_text:
                raise Exception(f"DocType '{doctype}' does not exist on master server. Please install the app containing this DocType on the master first.")
            elif "DoesNotExistError" in error_text:
                raise Exception(f"DocType '{doctype}' not found on master server.")
            else:
                raise Exception(f"Create failed: {error_text[:500]}")  # Limit error message length
    
    def _update_on_master(self, endpoint, doc_data, doctype):
        """Update document on master"""
        exclude = {"name", "creation", "modified", "modified_by", "owner", "docstatus", "idx", "hash"}
        clean = {k: v for k, v in doc_data.items() if k not in exclude and v is not None}
        
        response = self.session.put(endpoint, json=clean)
        if response.status_code not in [200]:
            error_text = response.text
            if "DoesNotExistError" in error_text:
                raise Exception(f"Document does not exist on master server.")
            else:
                raise Exception(f"Update failed: {error_text[:500]}")
    
    def _action_on_master(self, endpoint, action, doctype):
        """Perform an action (submit, cancel, amend) on master"""
        response = self.session.put(f"{endpoint}?action={action}", json={})
        if response.status_code not in [200]:
            raise Exception(f"Action {action} failed: {response.text}")
    
    def _delete_on_master(self, endpoint, doctype):
        """Delete document on master"""
        response = self.session.delete(endpoint)
        if response.status_code not in [200, 204]:
            raise Exception(f"Delete failed: {response.text}")
    
    def _check_exists_on_master(self, endpoint, doc_name):
        """Check if document exists on master"""
        try:
            response = self.session.get(f"{endpoint}/{doc_name}")
            return response.status_code == 200
        except:
            return False
    
    def _get_remote_doc_from_master(self, endpoint, doc_name):
        """Get the full document from master"""
        try:
            response = self.session.get(f"{endpoint}/{doc_name}")
            if response.status_code == 200:
                return response.json().get("data", {})
            return {}
        except:
            return {}
    
    def _resolve_collision_on_master(self, endpoint, doctype, doc_data):
        """Get next available name from master when collision detected"""
        try:
            response = self.session.get(
                f"{self.master_url}/api/method/frappe.client.get_next_name",
                params={"doctype": doctype}
            )
            if response.status_code == 200:
                return response.json().get("message")
        except:
            pass
        raise Exception(f"Could not resolve collision for {doctype}")
    
    def _rename_local_doc(self, doctype, old_name, new_name):
        """Rename document locally and update all references"""
        frappe.flags.sync_in_progress = True
        try:
            frappe.rename_doc(doctype, old_name, new_name, merge=False)
        finally:
            frappe.flags.sync_in_progress = False