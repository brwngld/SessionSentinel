(function () {
  let logsHydrated = false;
  let jobsHydrated = false;
  let fileExpiryIntervalStarted = false;
  const filePanelState = {};

  function getCSRFToken() {
    // Try to find CSRF token from any form input
    let token = document.querySelector('input[name="csrf_token"]')?.value;
    if (token) return token;
    
    // Try from data attributes
    token = document.querySelector('[data-csrf]')?.getAttribute('data-csrf');
    if (token) return token;
    
    // Try from meta tag (if we add one)
    token = document.querySelector('meta[name="csrf-token"]')?.getAttribute('content');
    if (token) return token;
    
    return '';
  }

  function getRetentionHours() {
    const jobsContainer = document.getElementById('jobs');
    const raw = jobsContainer ? jobsContainer.getAttribute('data-retention-hours') : '24';
    const hours = Number(raw || 24);
    return Number.isFinite(hours) && hours > 0 ? hours : 24;
  }

  function initSavedCarousel() {
    const carousel = document.getElementById('saved-carousel');
    if (!carousel) {
      return;
    }

    const track = carousel.querySelector('.saved-track');
    const prevBtn = carousel.querySelector('[data-carousel-prev]');
    const nextBtn = carousel.querySelector('[data-carousel-next]');
    const dotsContainer = document.getElementById('saved-dots');
    const positionEl = document.getElementById('saved-position');
    if (!track || !prevBtn || !nextBtn || !dotsContainer || !positionEl) {
      return;
    }

    const slides = Array.from(track.querySelectorAll('.saved-slide'));
    const itemsPerPage = 2;
    const totalPages = Math.ceil(slides.length / itemsPerPage);

    if (totalPages <= 1) {
      prevBtn.style.display = 'none';
      nextBtn.style.display = 'none';
      positionEl.textContent = '1 / 1';
      return;
    }

    let currentPage = 0;
    let autoplayTimer = null;

    function render() {
      track.style.transform = 'translateX(-' + (currentPage * 100) + '%)';
      positionEl.textContent = String(currentPage + 1) + ' / ' + String(totalPages);
      const dots = dotsContainer.querySelectorAll('.saved-dot');
      dots.forEach(function (dot, idx) {
        dot.classList.toggle('active', idx === currentPage);
      });
    }

    function goTo(pageIndex) {
      const max = totalPages - 1;
      if (pageIndex < 0) {
        currentPage = max;
      } else if (pageIndex > max) {
        currentPage = 0;
      } else {
        currentPage = pageIndex;
      }
      render();
    }

    function stopAutoplay() {
      if (autoplayTimer) {
        clearInterval(autoplayTimer);
        autoplayTimer = null;
      }
    }

    function startAutoplay() {
      stopAutoplay();
      autoplayTimer = setInterval(function () {
        goTo(currentPage + 1);
      }, 4500);
    }

    dotsContainer.innerHTML = '';
    for (let idx = 0; idx < totalPages; idx += 1) {
      const dot = document.createElement('button');
      dot.type = 'button';
      dot.className = 'saved-dot' + (idx === 0 ? ' active' : '');
      dot.setAttribute('aria-label', 'Go to saved page ' + String(idx + 1));
      dot.addEventListener('click', function () {
        goTo(idx);
        startAutoplay();
      });
      dotsContainer.appendChild(dot);
    }

    prevBtn.addEventListener('click', function () {
      goTo(currentPage - 1);
      startAutoplay();
    });

    nextBtn.addEventListener('click', function () {
      goTo(currentPage + 1);
      startAutoplay();
    });

    carousel.addEventListener('mouseenter', stopAutoplay);
    carousel.addEventListener('mouseleave', startAutoplay);

    document.addEventListener('visibilitychange', function () {
      if (document.hidden) {
        stopAutoplay();
      } else {
        startAutoplay();
      }
    });

    render();
    startAutoplay();
  }

  function initDatePickers() {
    if (!window.flatpickr) {
      return;
    }
    flatpickr('.js-date-input', {
      allowInput: true,
      altInput: true,
      altFormat: 'd/m/y',
      dateFormat: 'Y-m-d',
      disableMobile: true,
    });
  }

  function formatLastUpdated() {
    const now = new Date();
    return now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  async function updateLogs() {
    const outcomeSelect = document.getElementById('log-outcome');
    const eventTypeSelect = document.getElementById('log-event-type');
    const container = document.getElementById('log-list');
    if (!outcomeSelect || !eventTypeSelect || !container) {
      return;
    }

    const outcome = outcomeSelect.value;
    const eventType = eventTypeSelect.value;
    const response = await fetch('/logs?outcome=' + encodeURIComponent(outcome) + '&event_type=' + encodeURIComponent(eventType));
    if (!response.ok) {
      return;
    }

    const data = await response.json();
    container.innerHTML = '';

    const availableTypes = data.available_event_types || [];
    const currentValue = eventTypeSelect.value;
    eventTypeSelect.innerHTML = '';
    const allOpt = document.createElement('option');
    allOpt.value = 'all';
    allOpt.textContent = 'All';
    eventTypeSelect.appendChild(allOpt);
    for (const type of availableTypes) {
      const opt = document.createElement('option');
      opt.value = String(type).toLowerCase();
      opt.textContent = String(type);
      eventTypeSelect.appendChild(opt);
    }
    eventTypeSelect.value = Array.from(eventTypeSelect.options).some((o) => o.value === currentValue)
      ? currentValue
      : 'all';

    const events = data.events || [];
    if (!events.length) {
      container.innerHTML = '<div class="empty-state">No logs for selected filters.</div>';
      return;
    }

    for (const event of events) {
      const item = document.createElement('article');
      item.className = 'log-item ' + (event.outcome || '').toLowerCase();

      const meta = document.createElement('div');
      meta.className = 'log-meta';
      meta.textContent = '[' + event.event_time + '] ' + event.event_type + ' | ' + event.outcome;

      const details = document.createElement('div');
      details.textContent = event.details || '';

      item.appendChild(meta);
      if (event.details) {
        item.appendChild(details);
      }
      container.appendChild(item);
    }

    logsHydrated = true;
    const logsStamp = document.getElementById('logs-last-updated');
    if (logsStamp) {
      logsStamp.textContent = 'Last updated: ' + formatLastUpdated();
    }
  }

  function applyJobFilters() {
    const searchInput = document.getElementById('job-search');
    if (!searchInput) {
      return;
    }

    const query = (searchInput.value || '').toLowerCase().trim();
    const statusChip = document.querySelector('.chip.active');
    const statusFilter = statusChip ? statusChip.getAttribute('data-job-filter') : 'all';

    const jobs = document.querySelectorAll('[data-job-id]');
    for (const jobEl of jobs) {
      const status = (jobEl.getAttribute('data-job-status') || '').toLowerCase();
      const text = jobEl.textContent.toLowerCase();
      const matchesQuery = !query || text.includes(query);
      const matchesStatus = statusFilter === 'all' || status === statusFilter;
      jobEl.style.display = matchesQuery && matchesStatus ? '' : 'none';
    }
  }

  function renderJobFiles(jobEl, job) {
    const existing = jobEl.querySelector('.job-result');
    if (!existing) {
      return;
    }

    // Get CSRF token using central function
    let csrfToken = getCSRFToken();
    
    // If still empty, try from delete-files-control before we clear it
    if (!csrfToken) {
      const deleteControlDiv = existing.querySelector('.delete-files-control');
      csrfToken = deleteControlDiv ? deleteControlDiv.getAttribute('data-csrf') : '';
    }

    existing.innerHTML = '';

    const result = job && job.result ? job.result : null;
    const files = result && result.files ? result.files : null;
    const fileKeys = files ? Object.keys(files) : [];
    const jobId = String(job && job.id ? job.id : (jobEl.getAttribute('data-job-id') || ''));
    if (!(jobId in filePanelState)) {
      filePanelState[jobId] = false;
    }

    const rows = document.createElement('div');
    rows.innerHTML = '<strong>Rows:</strong> ' + String((result && result.row_count) || 0);
    existing.appendChild(rows);

    // Create controls wrapper (toggle button + delete button side by side)
    const controlsWrapper = document.createElement('div');
    controlsWrapper.className = 'files-controls-wrapper';
    existing.appendChild(controlsWrapper);

    if (!fileKeys.length) {
      const noFiles = document.createElement('div');
      noFiles.className = 'muted-inline';
      noFiles.textContent = 'No generated files';
      existing.appendChild(noFiles);
      
      // Still show delete button even if no files
      const deleteControl = document.createElement('div');
      const deleteForm = document.createElement('form');
      deleteForm.className = 'inline-form delete-files-form';
      deleteForm.method = 'post';
      deleteForm.action = '/files/' + encodeURIComponent(jobId) + '/delete';
      
      const csrfInput = document.createElement('input');
      csrfInput.type = 'hidden';
      csrfInput.name = 'csrf_token';
      csrfInput.value = csrfToken;
      deleteForm.appendChild(csrfInput);
      
      const deleteBtn = document.createElement('button');
      deleteBtn.className = 'delete-files-btn';
      deleteBtn.type = 'submit';
      deleteBtn.textContent = 'Delete all files';
      deleteForm.appendChild(deleteBtn);
      
      deleteControl.appendChild(deleteForm);
      controlsWrapper.appendChild(deleteControl);
      
      // Add delete run button
      const deleteRunControl = document.createElement('div');
      const deleteRunForm = document.createElement('form');
      deleteRunForm.className = 'inline-form delete-run-form';
      deleteRunForm.method = 'post';
      deleteRunForm.action = '/run/' + encodeURIComponent(jobId) + '/delete';
      deleteRunForm.onsubmit = function(e) {
        if (!window.confirm('Permanently delete this entire run? This cannot be undone.')) {
          e.preventDefault();
        }
      };
      
      const csrfInput2 = document.createElement('input');
      csrfInput2.type = 'hidden';
      csrfInput2.name = 'csrf_token';
      csrfInput2.value = csrfToken;
      deleteRunForm.appendChild(csrfInput2);
      
      const deleteRunBtn = document.createElement('button');
      deleteRunBtn.className = 'delete-run-btn';
      deleteRunBtn.type = 'submit';
      deleteRunBtn.textContent = 'Delete Run';
      deleteRunForm.appendChild(deleteRunBtn);
      
      deleteRunControl.appendChild(deleteRunForm);
      controlsWrapper.appendChild(deleteRunControl);
      return;
    }

    const toggleBtn = document.createElement('button');
    toggleBtn.type = 'button';
    toggleBtn.className = 'files-toggle-btn';
    controlsWrapper.appendChild(toggleBtn);

    // Create delete button in controls wrapper
    const deleteControl = document.createElement('div');
    const deleteForm = document.createElement('form');
    deleteForm.className = 'inline-form delete-files-form';
    deleteForm.method = 'post';
    deleteForm.action = '/files/' + encodeURIComponent(jobId) + '/delete';
    
    const csrfInput = document.createElement('input');
    csrfInput.type = 'hidden';
    csrfInput.name = 'csrf_token';
    csrfInput.value = csrfToken;
    deleteForm.appendChild(csrfInput);
    
    const deleteBtn = document.createElement('button');
    deleteBtn.className = 'delete-files-btn';
    deleteBtn.type = 'submit';
    deleteBtn.textContent = 'Delete all files';
    deleteForm.appendChild(deleteBtn);
    
    deleteControl.appendChild(deleteForm);
    controlsWrapper.appendChild(deleteControl);

    // Create delete run button in controls wrapper
    const deleteRunControl = document.createElement('div');
    const deleteRunForm = document.createElement('form');
    deleteRunForm.className = 'inline-form delete-run-form';
    deleteRunForm.method = 'post';
    deleteRunForm.action = '/run/' + encodeURIComponent(jobId) + '/delete';
    deleteRunForm.onsubmit = function(e) {
      if (!window.confirm('Permanently delete this entire run? This cannot be undone.')) {
        e.preventDefault();
      }
    };
    
    const csrfInput2 = document.createElement('input');
    csrfInput2.type = 'hidden';
    csrfInput2.name = 'csrf_token';
    csrfInput2.value = csrfToken;
    deleteRunForm.appendChild(csrfInput2);
    
    const deleteRunBtn = document.createElement('button');
    deleteRunBtn.className = 'delete-run-btn';
    deleteRunBtn.type = 'submit';
    deleteRunBtn.textContent = 'Delete Run';
    deleteRunForm.appendChild(deleteRunBtn);
    
    deleteRunControl.appendChild(deleteRunForm);
    controlsWrapper.appendChild(deleteRunControl);

    const container = document.createElement('div');
    container.className = 'files-table-wrap';
    existing.appendChild(container);

    const table = document.createElement('table');
    table.className = 'job-files-table';
    const thead = document.createElement('thead');
    thead.innerHTML = '<tr><th>Format</th><th>File Name</th><th>Actions</th><th>Expiry</th></tr>';
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    for (const key of fileKeys) {
      const fileData = files[key] || {};
      const name = typeof fileData === 'string' ? fileData : (fileData.name || key.toUpperCase());
      const createdAt = typeof fileData === 'object' ? fileData.created_at : '';

      const tr = document.createElement('tr');
      const formatCell = document.createElement('td');
      formatCell.textContent = key.toUpperCase();

      const nameCell = document.createElement('td');
      nameCell.textContent = name;

      const actionsCell = document.createElement('td');

      const openLink = document.createElement('a');
      openLink.href = '/view/' + encodeURIComponent(job.id) + '/' + encodeURIComponent(key);
      openLink.textContent = 'Open';
      openLink.className = 'file-link file-link-open';
      actionsCell.appendChild(openLink);

      actionsCell.appendChild(document.createTextNode(' '));

      const downloadLink = document.createElement('a');
      downloadLink.href = '/download/' + encodeURIComponent(job.id) + '/' + encodeURIComponent(key);
      downloadLink.textContent = 'Download';
      downloadLink.className = 'file-link file-link-download';
      actionsCell.appendChild(downloadLink);

      const expiryCell = document.createElement('td');
      expiryCell.className = 'file-expiry-cell';
      if (createdAt) {
        const createdDate = new Date(createdAt);
        if (Number.isFinite(createdDate.getTime())) {
          const retentionHours = getRetentionHours();
          const expiryAt = new Date(createdDate.getTime() + retentionHours * 60 * 60 * 1000);
          expiryCell.setAttribute('data-expiry-at', expiryAt.toISOString());
          updateSingleExpiryCell(expiryCell);
        } else {
          expiryCell.textContent = 'Unknown';
        }
      } else {
        expiryCell.textContent = 'Unknown';
      }

      tr.appendChild(formatCell);
      tr.appendChild(nameCell);
      tr.appendChild(actionsCell);
      tr.appendChild(expiryCell);
      tbody.appendChild(tr);
    }

    table.appendChild(tbody);
    container.appendChild(table);

    function applyPanelState() {
      const expanded = Boolean(filePanelState[jobId]);
      container.style.display = expanded ? '' : 'none';
      const fileCount = fileKeys.length;
      toggleBtn.textContent = expanded ? `Hide Files (${fileCount})` : `Show Files (${fileCount})`;
    }

    toggleBtn.addEventListener('click', function () {
      filePanelState[jobId] = !filePanelState[jobId];
      applyPanelState();
    });

    applyPanelState();
  }

  function formatDuration(ms) {
    const totalSeconds = Math.floor(ms / 1000);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    return String(hours).padStart(2, '0') + ':' + String(minutes).padStart(2, '0') + ':' + String(seconds).padStart(2, '0');
  }

  function updateSingleExpiryCell(expiryCell) {
    const raw = expiryCell.getAttribute('data-expiry-at') || '';
    if (!raw) {
      expiryCell.textContent = 'No files';
      return;
    }
    const expiryTime = new Date(raw).getTime();
    if (!Number.isFinite(expiryTime)) {
      expiryCell.textContent = 'Unknown';
      return;
    }
    const remaining = expiryTime - Date.now();
    if (remaining <= 0) {
      expiryCell.textContent = 'Expired (cleanup pending)';
      return;
    }
    expiryCell.textContent = formatDuration(remaining);
  }

  function updateAllExpiryTimers() {
    const cells = document.querySelectorAll('.file-expiry-cell');
    for (const cell of cells) {
      updateSingleExpiryCell(cell);
    }
  }

  function updateJobMode(jobEl, job) {
    const modeEl = jobEl.querySelector('.run-mode');
    if (!modeEl) {
      return;
    }
    const headless = Boolean(job && job.payload && job.payload.headless);
    modeEl.textContent = headless ? 'Headless' : 'Visible';
    modeEl.classList.toggle('run-mode-headless', headless);
    modeEl.classList.toggle('run-mode-visible', !headless);
  }

  function updateStopControls(jobEl, job) {
    const control = jobEl.querySelector('.job-stop-control');
    if (!control) {
      return;
    }

    const status = String((job && job.status) || '').toLowerCase();
    control.innerHTML = '';

    if (status === 'queued' || status === 'running') {
      const jobsContainer = document.getElementById('jobs');
      const csrfInput = jobsContainer ? jobsContainer.querySelector('input[name="csrf_token"]') : null;
      const csrfToken = csrfInput ? csrfInput.value : '';
      if (!csrfToken) {
        return;
      }

      const form = document.createElement('form');
      form.className = 'inline-form stop-run-form';
      form.method = 'post';
      form.action = '/run/' + encodeURIComponent(job.id) + '/stop';

      const hidden = document.createElement('input');
      hidden.type = 'hidden';
      hidden.name = 'csrf_token';
      hidden.value = csrfToken;
      form.appendChild(hidden);

      const btn = document.createElement('button');
      btn.type = 'submit';
      btn.className = 'stop-run-btn';
      btn.textContent = 'Stop Run';
      form.appendChild(btn);

      control.appendChild(form);
      return;
    }

    if (status === 'stopping') {
      const span = document.createElement('span');
      span.className = 'stop-run-pending';
      span.textContent = 'Stopping...';
      control.appendChild(span);
      return;
    }

    if (status === 'failed' || status === 'stopped') {
      const jobsContainer = document.getElementById('jobs');
      const csrfInput = jobsContainer ? jobsContainer.querySelector('input[name="csrf_token"]') : null;
      const csrfToken = csrfInput ? csrfInput.value : '';
      if (!csrfToken) {
        return;
      }

      const form = document.createElement('form');
      form.className = 'inline-form retry-run-form';
      form.method = 'post';
      form.action = '/run/' + encodeURIComponent(job.id) + '/retry';

      const hidden = document.createElement('input');
      hidden.type = 'hidden';
      hidden.name = 'csrf_token';
      hidden.value = csrfToken;
      form.appendChild(hidden);

      const btn = document.createElement('button');
      btn.type = 'submit';
      btn.className = 'retry-run-btn';
      btn.textContent = 'Retry Run';
      form.appendChild(btn);

      control.appendChild(form);
    }
  }

  async function updateJobs() {
    const jobs = document.querySelectorAll('[data-job-id]');
    if (!jobs.length) {
      return;
    }

    if (!jobsHydrated) {
      for (const jobEl of jobs) {
        const status = jobEl.querySelector('.status');
        if (status) {
          status.classList.add('loading-status');
        }
      }
    }

    // OPTIMIZATION: Fetch all jobs in one batch request instead of individual requests
    try {
      const response = await fetch('/jobs/status/batch');
      if (!response.ok) {
        return;
      }
      const data = await response.json();
      const allJobsData = data.jobs || [];
      const jobsMap = {};
      for (const jobData of allJobsData) {
        jobsMap[jobData.id] = jobData;
      }

      for (const jobEl of jobs) {
        const jobId = jobEl.getAttribute('data-job-id');
        const job = jobsMap[jobId];
        if (!job) {
          continue;
        }

        const status = jobEl.querySelector('.status');
        const message = jobEl.querySelector('.message');
        if (status) {
          status.textContent = job.status;
          status.className = 'status ' + job.status;
        }
        jobEl.setAttribute('data-job-status', job.status || '');
        if (message) {
          if ((job.status || '').toLowerCase() === 'stopped') {
            const rowCount = job.result && typeof job.result.row_count !== 'undefined' ? Number(job.result.row_count || 0) : 0;
            const files = job.result && job.result.files ? Object.keys(job.result.files).length : 0;
            const base = job.last_message || 'Run stopped by user';
            message.textContent = base + ' | Rows: ' + rowCount + ' | Files: ' + files;
          } else {
            message.textContent = job.last_message || 'No status message available yet';
          }
        }

        updateStopControls(jobEl, job);
        updateJobMode(jobEl, job);
        renderJobFiles(jobEl, job);
      }
    } catch (err) {
      console.error('Error fetching jobs batch:', err);
    }

    jobsHydrated = true;
    const jobsStamp = document.getElementById('jobs-last-updated');
    if (jobsStamp) {
      jobsStamp.textContent = 'Last updated: ' + formatLastUpdated();
    }
    applyJobFilters();
  }

  function bindEvents() {
    const outcomeSelect = document.getElementById('log-outcome');
    const eventTypeSelect = document.getElementById('log-event-type');
    const searchInput = document.getElementById('job-search');
    const focusJobsButton = document.getElementById('focus-jobs');

    if (outcomeSelect) {
      outcomeSelect.addEventListener('change', updateLogs);
    }
    if (eventTypeSelect) {
      eventTypeSelect.addEventListener('change', updateLogs);
    }
    if (searchInput) {
      searchInput.addEventListener('input', applyJobFilters);
    }
    if (focusJobsButton) {
      focusJobsButton.addEventListener('click', function () {
        const activity = document.getElementById('activity');
        if (activity) {
          activity.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
        applyJobFilters();
      });
    }

    for (const chip of document.querySelectorAll('.chip')) {
      chip.addEventListener('click', function () {
        for (const c of document.querySelectorAll('.chip')) {
          c.classList.remove('active');
        }
        chip.classList.add('active');
        applyJobFilters();
      });
    }

    document.addEventListener('submit', function (event) {
      const form = event.target;
      if (!(form instanceof HTMLFormElement) || !form.classList.contains('delete-files-form')) {
        return;
      }

      const action = form.getAttribute('action') || '';
      const parts = action.split('/');
      const jobId = parts.length >= 3 ? parts[2] : 'this run';
      const confirmed = window.confirm('Delete all generated files for run ' + jobId + '? This cannot be undone.');
      if (!confirmed) {
        event.preventDefault();
      }
    });
  }

  function init() {
    initDatePickers();
    initSavedCarousel();
    bindEvents();
    updateLogs();
    updateJobs();
    applyJobFilters();
    setInterval(function () {
      updateLogs();
      updateJobs();
    }, 3000);

    if (!fileExpiryIntervalStarted) {
      fileExpiryIntervalStarted = true;
      setInterval(updateAllExpiryTimers, 1000);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
