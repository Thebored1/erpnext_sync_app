// FILE: apps/sync_app/sync_app/public/js/sync_global.js

// We use $(function() { ... }) instead of frappe.ready() 
// because it is standard jQuery and works 100% of the time.
$(function() {
    // Safety check: Ensure 'frappe' object exists before using it
    if (typeof frappe === 'undefined' || typeof frappe.call === 'undefined') {
        console.warn("Sync App: Frappe not loaded yet.");
        return;
    }

    // 1. Check if user is logged in
    if (frappe.session.user === 'Guest') return;

    console.log("Sync App: Initializing...");

    // 2. Define the Sync Logic
    const run_sync_up = function() {
        frappe.confirm('Push changes to Master?', () => {
            frappe.call({
                method: 'sync_app.sync.api.sync_up_to_master',
                freeze: true,
                freeze_message: 'Pushing...',
                callback: (r) => { if(!r.exc) frappe.msgprint(r.message); }
            });
        });
    };

    const run_sync_down = function() {
        frappe.confirm('Pull changes from Master?', () => {
            frappe.call({
                method: 'sync_app.sync.api.sync_down_from_master',
                freeze: true,
                freeze_message: 'Pulling...',
                callback: (r) => { if(!r.exc) frappe.msgprint(r.message); }
            });
        });
    };

    // 3. Create the Floating Button
    const create_floating_button = function() {
        // Remove existing button if it exists
        $('#sync-floating-btn').remove();

        const btn_html = `
            <div id="sync-floating-btn" style="position: fixed; bottom: 30px; right: 30px; z-index: 9999;">
                <button class="btn btn-primary shadow" style="border-radius: 50%; width: 60px; height: 60px; font-size: 24px; display: flex; align-items: center; justify-content: center;">
                    <i class="fa fa-refresh"></i>
                </button>
                <div id="sync-popup-menu" class="shadow" style="display: none; position: absolute; bottom: 70px; right: 0; background: white; border: 1px solid #ddd; border-radius: 8px; width: 200px; padding: 0; overflow: hidden;">
                    <a href="#" id="float-sync-up" style="display: block; padding: 12px; color: #333; text-decoration: none; border-bottom: 1px solid #eee; font-size: 14px;">
                        <i class="fa fa-arrow-up" style="margin-right: 8px; color: #5e64ff;"></i> Sync Up (Push)
                    </a>
                    <a href="#" id="float-sync-down" style="display: block; padding: 12px; color: #333; text-decoration: none; font-size: 14px;">
                        <i class="fa fa-arrow-down" style="margin-right: 8px; color: #ffa00a;"></i> Sync Down (Pull)
                    </a>
                </div>
            </div>
        `;

        $('body').append(btn_html);

        // Toggle menu on click
        $('#sync-floating-btn button').on('click', function(e) {
            e.stopPropagation();
            $('#sync-popup-menu').toggle();
        });

        // Attach actions
        $('#float-sync-up').on('click', function(e) {
            e.preventDefault();
            $('#sync-popup-menu').hide();
            run_sync_up();
        });

        $('#float-sync-down').on('click', function(e) {
            e.preventDefault();
            $('#sync-popup-menu').hide();
            run_sync_down();
        });

        // Close menu when clicking anywhere else
        $(document).on('click', function() {
            $('#sync-popup-menu').hide();
        });
        
        console.log("Sync App: Floating button created");
    };

    // 4. Run creation
    create_floating_button();
});