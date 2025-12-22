(function () {
  const cfg = JSON.parse(document.getElementById('med-boot')?.textContent || '{}');
  const apiBase = cfg.apiBase || '';

  // State
  const state = {
    requirements: [],
    currentEditingId: null
  };

  // Load requirements from localStorage on init
  function loadRequirements() {
    try {
      const stored = localStorage.getItem('hims_requirements');
      if (stored) {
        state.requirements = JSON.parse(stored);
      }
    } catch (e) {
      console.error('Failed to load requirements', e);
    }
  }

  // Save requirements to localStorage
  function saveRequirements() {
    try {
      localStorage.setItem('hims_requirements', JSON.stringify(state.requirements));
    } catch (e) {
      console.error('Failed to save requirements', e);
    }
  }

  // Generate unique ID
  function generateId() {
    return 'req_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
  }

  // Calculate next due date
  function calculateNextDue(firstDue, frequency, dueTiming) {
    const now = new Date();
    const first = new Date(firstDue);
    
    let current = new Date(first);
    const frequencies = {
      'monthly': 1,
      'quarterly': 3,
      'semi-annual': 6,
      'annual': 12
    };
    
    const monthsToAdd = frequencies[frequency] || 1;
    
    // Find the next due date after now
    while (current < now) {
      current.setMonth(current.getMonth() + monthsToAdd);
    }
    
    // Apply due timing
    if (dueTiming === 'end') {
      // Set to last day of the month
      current = new Date(current.getFullYear(), current.getMonth() + 1, 0);
    } else if (dueTiming === 'beginning') {
      // Set to first day of the month
      current = new Date(current.getFullYear(), current.getMonth(), 1);
    }
    // 'exact' keeps the date as-is
    
    return current.toISOString().split('T')[0];
  }

  // Format date for display
  function formatDate(dateStr) {
    if (!dateStr) return '—';
    const d = new Date(dateStr);
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  }

  // Format datetime for display
  function formatDateTime(dateTimeStr) {
    if (!dateTimeStr) return '—';
    const d = new Date(dateTimeStr);
    return d.toLocaleDateString('en-US', { 
      month: 'short', 
      day: 'numeric', 
      year: 'numeric',
      hour: 'numeric',
      minute: '2-digit'
    });
  }

  // Check if requirement is overdue
  function isOverdue(req) {
    if (req.completed) return false;
    const now = new Date();
    const due = new Date(req.nextDue);
    return due < now;
  }

  // Render requirements table
  function renderRequirements() {
    const tbody = document.getElementById('requirements-body');
    
    if (state.requirements.length === 0) {
      tbody.innerHTML = `
        <tr>
          <td colspan="5" style="text-align:center;padding:32px;color:var(--muted)">
            No requirements added yet. Click "Add Requirement" to get started.
          </td>
        </tr>
      `;
      updateStats();
      return;
    }

    // Sort by next due date
    const sorted = [...state.requirements].sort((a, b) => {
      if (a.completed && !b.completed) return 1;
      if (!a.completed && b.completed) return -1;
      return new Date(a.nextDue) - new Date(b.nextDue);
    });

    tbody.innerHTML = sorted.map(req => {
      const overdue = isOverdue(req);
      let statusHtml = '';
      
      if (req.completed) {
        statusHtml = '<span class="status-badge status-completed">Completed</span>';
      } else if (overdue) {
        statusHtml = '<span class="status-badge status-overdue">Overdue</span>';
      } else if (req.type === 'appointment' || req.type === 'both') {
        if (req.appointmentDate) {
          statusHtml = `<span class="status-badge status-scheduled">Scheduled: ${formatDateTime(req.appointmentDate)}</span>`;
        } else {
          statusHtml = '<span class="status-badge status-not-scheduled">Not Scheduled</span>';
        }
      } else if (req.type === 'document') {
        if (req.documents && req.documents.length > 0) {
          statusHtml = '<span class="status-badge status-scheduled">Document Uploaded</span>';
        } else {
          statusHtml = '<span class="status-badge status-not-scheduled">No Document</span>';
        }
      }

      const typeDisplay = {
        'appointment': 'Appointment',
        'document': 'Document',
        'both': 'Appt + Doc'
      }[req.type] || req.type;

      const frequencyDisplay = {
        'monthly': 'Monthly',
        'quarterly': 'Quarterly',
        'semi-annual': 'Semi-Annual',
        'annual': 'Annual'
      }[req.frequency] || req.frequency;

      return `
        <tr class="req-row" data-req-id="${esc(req.id)}">
          <td><strong>${esc(req.name)}</strong></td>
          <td>${esc(typeDisplay)}</td>
          <td>${esc(frequencyDisplay)}</td>
          <td>${formatDate(req.nextDue)}</td>
          <td>${statusHtml}</td>
        </tr>
      `;
    }).join('');

    updateStats();
  }

  // Update statistics chips
  function updateStats() {
    const upcoming = state.requirements.filter(r => !r.completed && !isOverdue(r)).length;
    const overdue = state.requirements.filter(r => !r.completed && isOverdue(r)).length;
    const completed = state.requirements.filter(r => r.completed).length;

    document.getElementById('upcoming-count').textContent = upcoming;
    document.getElementById('overdue-count').textContent = overdue;
    document.getElementById('complete-count').textContent = completed;

    const overdueChip = document.getElementById('overdue-chip');
    if (overdue > 0) {
      overdueChip.style.display = '';
      overdueChip.style.background = 'rgba(255, 107, 107, 0.2)';
      overdueChip.style.borderColor = 'rgba(255, 107, 107, 0.4)';
    } else {
      overdueChip.style.display = 'none';
    }
  }

  // Open add requirement modal
  function openAddModal() {
    state.currentEditingId = null;
    document.getElementById('modal-title').textContent = 'Add Requirement';
    document.getElementById('requirement-form').reset();
    
    // Set default first due date to end of current month
    const now = new Date();
    const endOfMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0);
    document.getElementById('req-first-due').valueAsDate = endOfMonth;
    
    document.getElementById('requirement-modal').classList.remove('hidden');
    document.body.classList.add('modal-open');
  }

  // Close requirement modal
  function closeRequirementModal() {
    document.getElementById('requirement-modal').classList.add('hidden');
    document.body.classList.remove('modal-open');
    state.currentEditingId = null;
  }

  // Save requirement
  function saveRequirement() {
    const name = document.getElementById('req-name').value.trim();
    const type = document.getElementById('req-type').value;
    const frequency = document.getElementById('req-frequency').value;
    const firstDue = document.getElementById('req-first-due').value;
    const dueTiming = document.getElementById('req-due-timing').value;
    const notes = document.getElementById('req-notes').value.trim();

    if (!name || !type || !frequency || !firstDue) {
      alert('Please fill in all required fields');
      return;
    }

    const nextDue = calculateNextDue(firstDue, frequency, dueTiming);

    if (state.currentEditingId) {
      // Update existing
      const idx = state.requirements.findIndex(r => r.id === state.currentEditingId);
      if (idx !== -1) {
        state.requirements[idx] = {
          ...state.requirements[idx],
          name,
          type,
          frequency,
          firstDue,
          dueTiming,
          nextDue,
          notes
        };
      }
    } else {
      // Create new
      const req = {
        id: generateId(),
        name,
        type,
        frequency,
        firstDue,
        dueTiming,
        nextDue,
        notes,
        completed: false,
        appointmentDate: null,
        appointmentLocation: null,
        documents: [],
        detailNotes: ''
      };
      state.requirements.push(req);
    }

    saveRequirements();
    renderRequirements();
    closeRequirementModal();
  }

  // Open details modal
  function openDetailsModal(reqId) {
    const req = state.requirements.find(r => r.id === reqId);
    if (!req) return;

    state.currentEditingId = reqId;
    document.getElementById('details-title').textContent = req.name;

    // Show/hide sections based on type
    const apptSection = document.getElementById('appointment-section');
    const docSection = document.getElementById('document-section');

    if (req.type === 'appointment') {
      apptSection.style.display = 'block';
      docSection.style.display = 'none';
    } else if (req.type === 'document') {
      apptSection.style.display = 'none';
      docSection.style.display = 'block';
    } else if (req.type === 'both') {
      apptSection.style.display = 'block';
      docSection.style.display = 'block';
    }

    // Populate appointment fields
    if (req.type === 'appointment' || req.type === 'both') {
      const apptInput = document.getElementById('appt-date');
      if (req.appointmentDate) {
        // Convert ISO to local datetime-local format
        const d = new Date(req.appointmentDate);
        const localISO = new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
        apptInput.value = localISO;
      } else {
        apptInput.value = '';
      }
      document.getElementById('appt-location').value = req.appointmentLocation || '';
    }

    // Populate document fields
    if (req.type === 'document' || req.type === 'both') {
      renderUploadedDocs(req);
    }

    // Populate notes and completion
    document.getElementById('detail-notes').value = req.detailNotes || '';
    document.getElementById('mark-complete').checked = req.completed || false;

    document.getElementById('details-modal').classList.remove('hidden');
    document.body.classList.add('modal-open');
  }

  // Close details modal
  function closeDetailsModal() {
    document.getElementById('details-modal').classList.add('hidden');
    document.body.classList.remove('modal-open');
    state.currentEditingId = null;
  }

  // Save details
  function saveDetails() {
    const req = state.requirements.find(r => r.id === state.currentEditingId);
    if (!req) return;

    // Save appointment info
    if (req.type === 'appointment' || req.type === 'both') {
      const apptDate = document.getElementById('appt-date').value;
      req.appointmentDate = apptDate ? new Date(apptDate).toISOString() : null;
      req.appointmentLocation = document.getElementById('appt-location').value.trim();
    }

    // Save notes
    req.detailNotes = document.getElementById('detail-notes').value.trim();

    // Save completion status
    const wasCompleted = req.completed;
    req.completed = document.getElementById('mark-complete').checked;

    // If just marked complete, update next due date
    if (!wasCompleted && req.completed) {
      req.nextDue = calculateNextDue(req.nextDue, req.frequency, req.dueTiming);
      req.completed = false; // Reset for next cycle
    }

    saveRequirements();
    renderRequirements();
    closeDetailsModal();
  }

  // Delete requirement
  function deleteRequirement() {
    if (!confirm('Are you sure you want to delete this requirement?')) return;
    
    state.requirements = state.requirements.filter(r => r.id !== state.currentEditingId);
    saveRequirements();
    renderRequirements();
    closeDetailsModal();
  }

  // Handle file upload
  function handleFileUpload(event) {
    const req = state.requirements.find(r => r.id === state.currentEditingId);
    if (!req) return;

    const file = event.target.files[0];
    if (!file) return;

    // In a real app, you'd upload to a server. For now, store filename and create blob URL
    const doc = {
      id: generateId(),
      name: file.name,
      size: file.size,
      uploadDate: new Date().toISOString(),
      // Note: In production, you'd upload to server and store URL
      // For now, we'll just store metadata
    };

    req.documents = req.documents || [];
    req.documents.push(doc);
    
    saveRequirements();
    renderUploadedDocs(req);
    
    // Clear input
    event.target.value = '';
  }

  // Render uploaded documents
  function renderUploadedDocs(req) {
    const container = document.getElementById('uploaded-docs');
    const docs = req.documents || [];
    
    if (docs.length === 0) {
      container.innerHTML = '<span class="muted" style="font-size:13px">No documents uploaded</span>';
      return;
    }

    container.innerHTML = docs.map(doc => `
      <div class="doc-item">
        <span style="color:var(--text)">${esc(doc.name)}</span>
        <button onclick="window.medPortal.removeDoc('${esc(doc.id)}')" title="Remove">✕</button>
      </div>
    `).join('');
  }

  // Remove document
  function removeDoc(docId) {
    const req = state.requirements.find(r => r.id === state.currentEditingId);
    if (!req) return;

    req.documents = (req.documents || []).filter(d => d.id !== docId);
    saveRequirements();
    renderUploadedDocs(req);
  }

  // Event listeners
  document.getElementById('add-requirement-btn').addEventListener('click', openAddModal);
  
  document.getElementById('req-close-1').addEventListener('click', closeRequirementModal);
  document.getElementById('req-close-2').addEventListener('click', closeRequirementModal);
  document.getElementById('req-save').addEventListener('click', saveRequirement);

  document.getElementById('details-close-1').addEventListener('click', closeDetailsModal);
  document.getElementById('details-close-2').addEventListener('click', closeDetailsModal);
  document.getElementById('details-save').addEventListener('click', saveDetails);
  document.getElementById('delete-req').addEventListener('click', deleteRequirement);

  document.getElementById('doc-upload').addEventListener('change', handleFileUpload);

  // Click on requirement row to open details
  document.getElementById('requirements-body').addEventListener('click', (e) => {
    const row = e.target.closest('tr.req-row');
    if (row) {
      const reqId = row.getAttribute('data-req-id');
      openDetailsModal(reqId);
    }
  });

  // Utility
  function esc(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  // Export functions to window for inline handlers
  window.medPortal = {
    removeDoc
  };

  // Initialize
  loadRequirements();
  renderRequirements();
})();