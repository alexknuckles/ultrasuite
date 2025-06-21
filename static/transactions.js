document.addEventListener('DOMContentLoaded', () => {
  const form = document.querySelector('form');
  const modal = document.getElementById('dateModal');
  const btn = document.getElementById('datePickerBtn');
  const dateLabel = document.getElementById('dateLabel');
  const tabs = document.querySelectorAll('#dateTabs li');
  const panes = document.querySelectorAll('.date-pane');
  const periodInput = document.getElementById('periodInput');
  const startInput = document.getElementById('startInput');
  const endInput = document.getElementById('endInput');
  const dayInput = document.getElementById('dayInput');
  const monthSelect = document.getElementById('monthSelect');
  const monthYearSelect = document.getElementById('monthYearSelect');
  const yearSelect = document.getElementById('yearSelect2');
  const startRange = document.getElementById('startInput2');
  const endRange = document.getElementById('endInput2');
  const apply = document.getElementById('applyDate');
  const cancel = document.getElementById('cancelDate');
  let activeType = '';

  const init = periodInput.value;
  if (init.startsWith('year-')) {
    activeType = 'year';
    yearSelect.value = init.split('-')[1];
  } else if (init.startsWith('month-')) {
    activeType = 'month';
    const p = init.split('-');
    monthYearSelect.value = p[1];
    monthSelect.value = p[2];
  } else if (init === 'last30') {
    activeType = 'last30';
  } else if (init === 'custom' && startInput.value && endInput.value) {
    if (startInput.value === endInput.value) {
      activeType = 'day';
      dayInput.value = startInput.value;
    } else {
      activeType = 'range';
      startRange.value = startInput.value;
      endRange.value = endInput.value;
    }
  } else if (startInput.value && endInput.value) {
    if (startInput.value === endInput.value) {
      activeType = 'day';
      dayInput.value = startInput.value;
    } else {
      activeType = 'range';
      startRange.value = startInput.value;
      endRange.value = endInput.value;
    }
  }

  function updateTabs() {
    tabs.forEach(t => {
      const type = t.dataset.type;
      const pane = document.getElementById('pane-' + (type || 'all'));
      if (type === activeType) {
        t.classList.add('is-active');
        pane.classList.remove('is-hidden');
      } else {
        t.classList.remove('is-active');
        pane.classList.add('is-hidden');
      }
    });
  }

  function showModal() {
    modal.classList.remove('is-hidden');
    modal.focus();
  }
  function hideModal() {
    modal.classList.add('is-hidden');
  }

  modal.addEventListener('click', e => {
    if (e.target === modal) hideModal();
  });
  btn.addEventListener('click', showModal);
  cancel.addEventListener('click', hideModal);

  document.addEventListener('keydown', e => {
    if (!modal.classList.contains('is-hidden')) {
      if (e.key === 'Escape') hideModal();
      if (e.key === 'Enter' && e.target.tagName !== 'INPUT') apply.click();
    }
  });

  tabs.forEach(t => {
    t.addEventListener('click', () => {
      activeType = t.dataset.type;
      updateTabs();
    });
  });

  function setButtonLabel() {
    let label = 'All';
    if (activeType === 'day' && dayInput.value) {
      label = dayInput.value;
    } else if (activeType === 'month' && monthYearSelect.value && monthSelect.value) {
      const monthName = monthSelect.options[monthSelect.selectedIndex].textContent;
      label = monthName + ' ' + monthYearSelect.value;
    } else if (activeType === 'year' && yearSelect.value) {
      label = yearSelect.value;
    } else if (activeType === 'range' && (startRange.value || endRange.value)) {
      label = (startRange.value || '') + ' - ' + (endRange.value || '');
    } else if (activeType === 'last30') {
      label = 'Last 30 days';
    }
    dateLabel.textContent = label;
  }

  apply.addEventListener('click', () => {
    if (activeType === 'day' && dayInput.value) {
      periodInput.value = 'custom';
      startInput.value = dayInput.value;
      endInput.value = dayInput.value;
    } else if (activeType === 'month' && monthYearSelect.value && monthSelect.value) {
      periodInput.value = 'month-' + monthYearSelect.value + '-' + monthSelect.value;
      startInput.value = '';
      endInput.value = '';
    } else if (activeType === 'year' && yearSelect.value) {
      periodInput.value = 'year-' + yearSelect.value;
      startInput.value = '';
      endInput.value = '';
    } else if (activeType === 'last30') {
      const endDt = new Date();
      const startDt = new Date();
      startDt.setDate(endDt.getDate() - 29);
      periodInput.value = 'last30';
      startInput.value = startDt.toISOString().slice(0, 10);
      endInput.value = endDt.toISOString().slice(0, 10);
    } else if (activeType === 'range' && (startRange.value || endRange.value)) {
      periodInput.value = 'custom';
      startInput.value = startRange.value;
      endInput.value = endRange.value;
    } else {
      periodInput.value = '';
      startInput.value = '';
      endInput.value = '';
    }
    setButtonLabel();
    hideModal();
    if (form) form.submit();
  });

  updateTabs();
  setButtonLabel();
});

