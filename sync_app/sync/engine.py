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
        
        # Create authenticated session
        self.session = requests.Session()
        self.session.auth = (self.api_key, self.api_secret)
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json"
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
        
        # Get MY device ID
        my_device_id = frappe.db.get_value("System Settings", None, "custom_device_id")
        
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
            except Exception as e:
                log_doc.sync_attempt_count = cint(log_doc.sync_attempt_count) + 1
                log_doc.sync_status = "failed"
                log_doc.error_message = str(e)[:500]
                log_doc.save(ignore_permissions=True)
                results["failed"] += 1
                results["errors"].append({
                    "log": log_entry.name,
                    "error": str(e),
                    "attempt": log_doc.sync_attempt_count
                })
        
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
            if remote_doc.get("creation") != doc_data.get("creation"):
                # Collision - rename
                final_name = self._resolve_collision_on_master(endpoint, doctype, doc_data)
                self._rename_local_doc(doctype, local_name, final_name)
                results["collisions_renamed"].append({
                    "doctype": doctype,
                    "original_name": local_name,
                    "renamed_to": final_name
                })
        
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
        
        # Get MY device ID
        my_device_id = frappe.db.get_value("System Settings", None, "custom_device_id")
        
        # Query MASTER for changes NOT from me
        try:
            response = self.session.get(
                f"{self.master_url}/api/resource/Sync Transaction Log",
                params={
                    "filters": [["device_id", "!=", my_device_id]],  # <-- NOT MINE
                    "fields": ["name", "doctype_name", "document_name", "operation", "doc_data", "device_id"],
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
                "errors": [],
                "direction": "down"
            }
            
            for remote_log in remote_logs:
                try:
                    self._apply_remote_change(remote_log)
                    results["synced"] += 1
                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append({
                        "doc": remote_log.get("document_name"),
                        "error": str(e)
                    })
            
            # Update last sync time
            config = frappe.get_doc("Sync Configuration")
            config.last_down_sync = frappe.utils.now()
            config.save(ignore_permissions=True)
            
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
            raise Exception(f"Create failed: {response.text}")
    
    def _update_on_master(self, endpoint, doc_data, doctype):
        """Update document on master"""
        exclude = {"name", "creation", "modified", "modified_by", "owner", "docstatus", "idx", "hash"}
        clean = {k: v for k, v in doc_data.items() if k not in exclude and v is not None}
        
        response = self.session.put(endpoint, json=clean)
        if response.status_code not in [200]:
            raise Exception(f"Update failed: {response.text}")
    
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