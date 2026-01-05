/**
 * Generic toggle between chart and table views on stats pages.
 *
 * Usage:
 *   <button
 *     data-toggle-chart="#chart-id"
 *     data-toggle-table="#table-id"
 *     data-chart-icon="fa-chart-bar"
 *     data-chart-label="Switch to Chart"
 *     data-table-label="Switch to Table">
 *   </button>
 */
(function () {
  function getElement(btn, attr) {
    var sel = btn.getAttribute(attr);
    return sel ? document.querySelector(sel) : null;
  }

  function initToggle(btn) {
    if (!btn || btn.getAttribute('data-toggle-init') === '1') {
      return;
    }

    var chartEl = getElement(btn, 'data-toggle-chart');
    var tableEl = getElement(btn, 'data-toggle-table');
    if (!chartEl || !tableEl) {
      return;
    }

    var chartLabel = btn.dataset.chartLabel || 'Switch to Chart';
    var tableLabel = btn.dataset.tableLabel || 'Switch to Table';
    var chartIcon = btn.dataset.chartIcon || 'fa-chart-bar';
    var tableIcon = btn.dataset.tableIcon || 'fa-table';
    var showingChart = true;

    btn.addEventListener('click', function () {
      if (showingChart) {
        chartEl.style.display = 'none';
        tableEl.style.display = 'block';
        btn.innerHTML = '<i class="fa ' + chartIcon + '"></i> ' + chartLabel;
      } else {
        chartEl.style.display = 'block';
        tableEl.style.display = 'none';
        btn.innerHTML = '<i class="fa ' + tableIcon + '"></i> ' + tableLabel;

        if (typeof echarts !== 'undefined') {
          var inst = echarts.getInstanceByDom(chartEl);
          if (inst) {
            inst.resize();
          }
        }
      }
      showingChart = !showingChart;
    });

    btn.setAttribute('data-toggle-init', '1');
  }

  document.addEventListener('DOMContentLoaded', function () {
    var buttons = document.querySelectorAll('[data-toggle-chart][data-toggle-table]');
    buttons.forEach(function (btn) {
      initToggle(btn);
    });
  });
})();

