# Copyright (c) 2025, Apstic and contributors
# For license information, please see license.txt

import frappe
import uuid
from frappe.model.document import Document


class SyncConfiguration(Document):
	def before_save(self):
		"""Auto-generate custom_device_id if not present"""
		if not self.custom_device_id:
			# Check if is_master is actually checked (True/1)
			if self.is_master == 1 or self.is_master is True:
				# Master server gets "MASTER" as device ID
				self.custom_device_id = "MASTER"
			else:
				# Offline/child instances get a unique 8-character ID
				self.custom_device_id = str(uuid.uuid4())[:8].upper()

