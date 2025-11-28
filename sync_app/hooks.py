app_name = "sync_app"
app_title = "Sync App"
app_publisher = "Apstic"
app_description = "N-way sync application for ERPNext installations."
app_email = "nites0262@gmail.com"
app_license = "mit"

doc_events = {
    "*": {
        "after_insert": "sync_app.sync.capture.capture_change",
        "after_save": "sync_app.sync.capture.capture_change",
        "after_submit": "sync_app.sync.capture.capture_change",
        "after_amend": "sync_app.sync.capture.capture_change",
        "after_cancel": "sync_app.sync.capture.capture_change",
        "before_delete": "sync_app.sync.capture.capture_change",
        "on_trash": "sync_app.sync.capture.capture_change",
    }
}

# Excluded from syncing
SYNC_EXCLUDED_DOCTYPES = [
    # Sync App Internal
    "Sync Transaction Log",
    "Sync Configuration",
    
    # System / Auth (Don't sync these!)
    "User",
    "Role",
    "Has Role",
    "Session",
    "Version",
    "Module Def",
    
    # Logs & Noise (The items cluttering your screen)
    "Route History",
    "Scheduled Job Log",
    "Comment",
    "Error Log",
    "Access Log",
    "Activity Log",
    "View Log",
    "Energy Point Log",
    
    # System Queues
    "Email Queue",
    "Submission Queue",
    "Unhandled Email",
    "System Console",
    "Database Console",
    "Prepared Report",
]

# Make sync endpoints available
fixtures = [
    {
        "doctype": "Custom Role",
        "filters": {
            "role_name": "Sync Manager"
        }
    }
]

app_include_js = [
    "/assets/sync_app/js/sync_global.js"  # Load this on all pages
]