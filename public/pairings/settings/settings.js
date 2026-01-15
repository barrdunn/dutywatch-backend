/* ================================================
   SETTINGS - Profile & Modal Logic
   ================================================ */
(function () {
  const cfg = window.dwConfig || {};
  const apiBase = cfg.apiBase || '';

  // =========================
  // Profile State
  // =========================
  let profileCache = { firstName: 'Barry', lastName: 'Dunn', photo: null };
  let profileSaveTimeout = null;

  async function loadProfile() {
    try {
      const res = await fetch(apiBase + '/api/profile');
      if (res.ok) {
        const data = await res.json();
        profileCache = {
          firstName: data.firstName || 'Barry',
          lastName: data.lastName || 'Dunn',
          photo: data.photo || null
        };
      }
    } catch (e) {
      console.error('Failed to load profile:', e);
    }
    return profileCache;
  }

  function saveProfile(profile) {
    // Update cache immediately for responsive UI
    profileCache = { ...profile };
    
    // Debounce the actual save to avoid hammering the server while typing
    if (profileSaveTimeout) clearTimeout(profileSaveTimeout);
    profileSaveTimeout = setTimeout(async () => {
      try {
        await fetch(apiBase + '/api/profile', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            firstName: profile.firstName,
            lastName: profile.lastName,
            photo: profile.photo
          })
        });
      } catch (e) {
        console.error('Failed to save profile:', e);
      }
    }, 500);
  }

  function getProfile() {
    return profileCache;
  }

  function getInitials(firstName, lastName) {
    const f = (firstName || '').trim().charAt(0).toUpperCase();
    const l = (lastName || '').trim().charAt(0).toUpperCase();
    return (f + l) || 'BD';
  }

  function updateAvatarDisplay(profile) {
    const headerAvatar = document.querySelector('#header-avatar');
    const settingsAvatar = document.querySelector('#settings-avatar');
    const displayName = document.querySelector('#display-name');
    const removeBtn = document.querySelector('#remove-photo-btn');

    const initials = getInitials(profile.firstName, profile.lastName);
    const fullName = [profile.firstName, profile.lastName].filter(Boolean).join(' ') || 'Barry Dunn';

    // Update display name
    if (displayName) {
      displayName.textContent = fullName;
    }

    // Update header avatar
    if (headerAvatar) {
      if (profile.photo) {
        headerAvatar.innerHTML = `<img src="${profile.photo}" alt="${fullName}">`;
      } else {
        headerAvatar.textContent = initials;
      }
    }

    // Update settings avatar
    if (settingsAvatar) {
      if (profile.photo) {
        settingsAvatar.innerHTML = `<img src="${profile.photo}" alt="${fullName}">`;
      } else {
        settingsAvatar.textContent = initials;
      }
    }

    // Show/hide remove photo button
    if (removeBtn) {
      removeBtn.classList.toggle('hidden', !profile.photo);
    }
  }

  // =========================
  // Settings Modal
  // =========================
  const settingsModal = document.querySelector('#settings-modal');
  const settingsClose = document.querySelector('#settings-close');
  const settingsDone = document.querySelector('#settings-done');
  const avatarBtn = document.querySelector('#avatar-btn');
  const profileAvatarBtn = document.querySelector('#profile-avatar-btn');
  const photoInput = document.querySelector('#photo-input');
  const removePhotoBtn = document.querySelector('#remove-photo-btn');
  const firstNameInput = document.querySelector('#first-name');
  const lastNameInput = document.querySelector('#last-name');
  const settingsRefreshSel = document.querySelector('#settings-refresh-mins');

  async function openSettings() {
    if (!settingsModal) return;
    
    // Load current profile from backend
    const profile = await loadProfile();
    if (firstNameInput) firstNameInput.value = profile.firstName || '';
    if (lastNameInput) lastNameInput.value = profile.lastName || '';
    updateAvatarDisplay(profile);
    
    // Fetch current refresh setting from backend
    try {
      const res = await fetch(apiBase + '/api/status');
      const data = await res.json();
      if (settingsRefreshSel && data.refresh_minutes) {
        settingsRefreshSel.value = String(data.refresh_minutes);
      }
    } catch (e) {
      console.error('Failed to fetch status:', e);
    }
    
    // Init clock toggle to current state
    initClockToggle();
    
    settingsModal.classList.remove('hidden');
    document.body.classList.add('modal-open');
  }

  function closeSettings() {
    if (!settingsModal) return;
    
    // Save profile when closing
    const profile = getProfile();
    profile.firstName = (firstNameInput?.value || '').trim() || 'Barry';
    profile.lastName = (lastNameInput?.value || '').trim() || 'Dunn';
    saveProfile(profile);
    updateAvatarDisplay(profile);
    
    settingsModal.classList.add('hidden');
    document.body.classList.remove('modal-open');
  }

  // Avatar button opens settings
  if (avatarBtn) {
    avatarBtn.addEventListener('click', openSettings);
  }

  // Close buttons
  if (settingsClose) {
    settingsClose.addEventListener('click', closeSettings);
  }
  if (settingsDone) {
    settingsDone.addEventListener('click', closeSettings);
  }

  // Profile photo click triggers file input
  if (profileAvatarBtn && photoInput) {
    profileAvatarBtn.addEventListener('click', () => {
      photoInput.click();
    });
  }

  // Handle photo selection
  if (photoInput) {
    photoInput.addEventListener('change', (e) => {
      const file = e.target.files?.[0];
      if (!file) return;
      
      const reader = new FileReader();
      reader.onload = (evt) => {
        const dataUrl = evt.target?.result;
        if (typeof dataUrl === 'string') {
          const profile = getProfile();
          profile.photo = dataUrl;
          saveProfile(profile);
          updateAvatarDisplay(profile);
        }
      };
      reader.readAsDataURL(file);
      
      // Reset input so same file can be selected again
      photoInput.value = '';
    });
  }

  // Remove photo button
  if (removePhotoBtn) {
    removePhotoBtn.addEventListener('click', () => {
      const profile = getProfile();
      profile.photo = null;
      saveProfile(profile);
      updateAvatarDisplay(profile);
    });
  }

  // Name inputs - save on change
  if (firstNameInput) {
    firstNameInput.addEventListener('input', () => {
      const profile = getProfile();
      profile.firstName = firstNameInput.value.trim();
      saveProfile(profile);
      updateAvatarDisplay(profile);
    });
  }

  if (lastNameInput) {
    lastNameInput.addEventListener('input', () => {
      const profile = getProfile();
      profile.lastName = lastNameInput.value.trim();
      saveProfile(profile);
      updateAvatarDisplay(profile);
    });
  }

  // Refresh interval in settings - update backend on change
  if (settingsRefreshSel) {
    settingsRefreshSel.addEventListener('change', async () => {
      const minutes = parseInt(settingsRefreshSel.value, 10);
      try {
        await fetch(apiBase + '/api/settings/refresh-seconds', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ seconds: minutes * 60 })
        });
      } catch (e) {
        console.error('Failed to update refresh interval:', e);
      }
    });
  }

  // =========================
  // Clock Mode Toggle
  // =========================
  const clockToggle = document.querySelector('#clock-mode');
  const clockLabels = document.querySelectorAll('.clock-label');

  function updateClockLabels(is24h) {
    clockLabels.forEach(label => {
      const mode = label.getAttribute('data-mode');
      if (mode === '24') {
        label.classList.toggle('active', is24h);
      } else if (mode === '12') {
        label.classList.toggle('active', !is24h);
      }
    });
  }

  function initClockToggle() {
    // Get current mode from pairings module (may not be loaded yet)
    const is24h = window.dwPairings?.getClockMode?.() === 24;
    if (clockToggle) {
      clockToggle.checked = is24h;
    }
    updateClockLabels(is24h);
  }

  if (clockToggle) {
    clockToggle.addEventListener('change', () => {
      const is24h = clockToggle.checked;
      updateClockLabels(is24h);
      // Update pairings display
      if (window.dwPairings?.setClockMode) {
        window.dwPairings.setClockMode(is24h ? 24 : 12);
      }
    });
  }

  // =========================
  // Initialize
  // =========================
  (async () => {
    const profile = await loadProfile();
    updateAvatarDisplay(profile);
  })();

  // Expose for other modules if needed
  window.dwSettings = {
    loadProfile,
    getProfile,
    saveProfile,
    updateAvatarDisplay,
    openSettings,
    closeSettings
  };
})();