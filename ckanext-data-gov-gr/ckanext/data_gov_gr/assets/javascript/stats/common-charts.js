(function (root) {
  var hasEcharts = typeof echarts !== 'undefined';
  var palette = [
    '#1f77b4',
    '#ff7f0e',
    '#2ca02c',
    '#d62728',
    '#9467bd',
    '#8c564b',
    '#e377c2',
    '#7f7f7f',
    '#bcbd22',
    '#17becf'
  ];

  function parseData(raw) {
    if (!raw) {
      return [];
    }
    if (typeof raw === 'string') {
      try {
        return JSON.parse(raw);
      } catch (e) {
        return [];
      }
    }
    return raw;
  }

  function parseObject(raw) {
    var parsed = parseData(raw);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return {};
    }
    return parsed;
  }

  function deepMerge(target, source) {
    if (!source) {
      return target;
    }
    Object.keys(source).forEach(function (key) {
      var value = source[key];
      if (value && typeof value === 'object' && !Array.isArray(value)) {
        if (!target[key] || typeof target[key] !== 'object' || Array.isArray(target[key])) {
          target[key] = {};
        }
        deepMerge(target[key], value);
      } else {
        target[key] = value;
      }
    });
    return target;
  }

  function buildPieOption(data, variant) {
    var dataset = Array.isArray(data) ? data : [];
    var showLegend = variant !== 'preview' || dataset.length <= 6;
    return {
      tooltip: {
        trigger: 'item',
        formatter: '{b}: {c} ({d}%)'
      },
      legend: {
        type: 'scroll',
        bottom: 0,
        show: showLegend
      },
      color: palette,
      series: [
        {
          type: 'pie',
          radius: ['40%', '68%'],
          center: ['50%', '45%'],
          label: { show: false },
          data: dataset,
          emphasis: {
            label: { show: true, fontSize: 14, fontWeight: 'bold' },
            itemStyle: {
              shadowBlur: 8,
              shadowOffsetX: 0,
              shadowColor: 'rgba(0,0,0,0.35)'
            }
          }
        }
      ]
    };
  }

  function buildBarOption(payload, el, variant) {
    var categories = (payload && payload.categories) || [];
    var series = (payload && payload.series) || [];

    var minHeight = 260;
    var calcHeight = Math.max(minHeight, (categories.length || 1) * 44 + 80);
    if (el) {
      el.style.height = calcHeight + 'px';
    }

    var containerWidth = (el && el.clientWidth) || 320;
    // In preview cards keep label column tighter so the bar area has enough width.
    var labelRatio = variant === 'preview' ? 0.42 : 0.35;
    var labelWidth = Math.min(variant === 'preview' ? 160 : 260, Math.max(110, Math.floor(containerWidth * labelRatio)));
    var gridLeftPx;
    var gridRightPx;
    if (variant === 'preview') {
      // In preview cards, keep bars aligned and avoid containLabel pushing the grid.
      gridLeftPx = labelWidth + 12;
      gridRightPx = 28;
    } else {
      gridLeftPx = labelWidth + 24;
      gridRightPx = 40;
    }

    var normalizedSeries = series.map(function (s) {
      return {
        name: s.name,
        type: 'bar',
        data: s.data,
        barMaxWidth: 22,
        label: {
          show: true,
          position: 'right',
          distance: 4
        }
      };
    });

    return {
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' }
      },
      legend: { show: normalizedSeries.length > 1 },
      color: palette,
      grid: {
        left: gridLeftPx,
        right: gridRightPx,
        bottom: '8%',
        top: '6%',
        containLabel: true
      },
      xAxis: {
        type: 'value',
        min: 0,
        splitNumber: variant === 'preview' ? 4 : 6,
        axisLabel: {
          hideOverlap: true,
          margin: variant === 'preview' ? 10 : 12,
          fontSize: variant === 'preview' ? 10 : 12
        },
        splitLine: {
          show: true,
          lineStyle: { type: 'dashed', opacity: 0.25 }
        }
      },
        yAxis: {
        type: 'category',
        inverse: true,
        data: categories,
        axisLabel: {
          interval: 0,
          width: labelWidth,
          overflow: 'truncate',
          margin: variant === 'preview' ? 12 : 18,
          formatter: function (value) {
            var text = String(value || '');
            var maxLen = variant === 'preview' ? 28 : 60;
            if (text.length <= maxLen) {
              return text;
            }
            return text.slice(0, maxLen - 1) + '…';
          }
        }
      },
      series: normalizedSeries
    };
  }

  function buildLineOption(payload, variant) {
    var xAxisType = (payload && payload.xAxisType) || 'category';
    var series = (payload && payload.series) || [];
    var isTime = xAxisType === 'time';

    var normalizedSeries = series.map(function (s, idx) {
      var seriesData = (s.data || []).map(function (point) {
        if (Array.isArray(point)) {
          return point;
        }
        if (point && typeof point === 'object') {
          var x = point[0] || point.x || point.date || point.name;
          var y = point[1] || point.y || point.value;
          return [x, y];
        }
        return point;
      });
      return {
        name: s.name || '',
        type: 'line',
        smooth: true,
        showSymbol: false,
        data: seriesData,
        areaStyle: idx === 0 ? { opacity: 0.15 } : undefined
      };
    });

    return {
      tooltip: {
        trigger: 'axis'
      },
      legend: {
        top: 0,
        show: variant !== 'preview' || normalizedSeries.length <= 1
      },
      color: palette,
      grid: {
        left: '6%',
        right: '6%',
        bottom: '14%',
        top: '10%',
        containLabel: true
      },
      xAxis: {
        type: xAxisType,
        boundaryGap: xAxisType === 'category',
        axisLabel: {
          hideOverlap: true,
          margin: variant === 'preview' ? 16 : 12,
          showMaxLabel: true,
          showMinLabel: true,
          rotate: variant === 'preview' ? 35 : 0,
          fontSize: variant === 'preview' ? 10 : 12,
          formatter: function (value) {
            if (!isTime) {
              return value;
            }
            try {
              var d = new Date(value);
              return d.toLocaleDateString(document.documentElement.lang || 'en', { year: 'numeric', month: 'short' });
            } catch (e) {
              return value;
            }
          }
        },
        minInterval: isTime ? 1000 * 60 * 60 * 24 * 30 : undefined
      },
      yAxis: {
        type: 'value',
        min: 0,
        splitLine: {
          show: true,
          lineStyle: { type: 'dashed', opacity: 0.25 }
        }
      },
      series: normalizedSeries
    };
  }

  function buildOption(type, data, el, variant) {
    if (type === 'bar') {
      return buildBarOption(data, el, variant);
    }
    if (type === 'line') {
      return buildLineOption(data, variant);
    }
    return buildPieOption(data, variant);
  }

  function getOptionsFromDataset(el) {
    var ds = el.dataset || {};
    return {
      type: ds.moduleType || ds.chartType || ds.type || 'pie',
      data: ds.moduleData || ds.data,
      title: ds.moduleTitle || ds.title || '',
      height: ds.moduleHeight || ds.height || '320px',
      variant: ds.moduleVariant || ds.variant || 'full',
      options: ds.moduleOptions || ds.options
    };
  }

  function renderCommonChart(el, opts) {
    if (!hasEcharts || !el || el.getAttribute('data-common-chart-init') === '1') {
      return;
    }
    var type = opts.type || 'pie';
    var data = parseData(opts.data);
    var height = opts.height || '320px';
    var variant = String(opts.variant || 'full').toLowerCase();
    var overrides = parseObject(opts.options);

    el.style.height = height;
    var chart = echarts.init(el);
    var option = buildOption(type, data, el, variant);
    option = deepMerge(option, overrides);
    if (option) {
      chart.setOption(option);
      chart.resize();
    }
    window.addEventListener('resize', function () {
      chart.resize();
    });
    el.setAttribute('data-common-chart-init', '1');
  }

  // CKAN module path
  if (root && root.ckan && root.ckan.module) {
    root.ckan.module('common-stats-charts', function (jQuery, _) {
      return {
        initialize: function () {
          if (!hasEcharts) {
            // eslint-disable-next-line no-console
            console.error('ECharts is required for common-stats-charts');
            return;
          }
          renderCommonChart(this.el[0], {
            type: this.options.type || this.options.chartType,
            data: this.options.data,
            title: this.options.title,
            height: this.options.height,
            variant: this.options.variant,
            options: this.options.options
          });
          this._boundResize = (this._boundResize || function () {
            var inst = echarts.getInstanceByDom(this.el[0]);
            if (inst) {
              inst.resize();
            }
          }.bind(this));
          jQuery(window).on('resize', this._boundResize);
        },
        teardown: function () {
          jQuery(window).off('resize', this._boundResize);
          var inst = echarts.getInstanceByDom(this.el[0]);
          if (inst) {
            inst.dispose();
          }
        }
      };
    });
  } else {
    // Fallback: render charts even if CKAN core JS is not present (e.g., embedded contexts)
    document.addEventListener('DOMContentLoaded', function () {
      var nodes = document.querySelectorAll('[data-module="common-stats-charts"]');
      if (!nodes.length) {
        return;
      }
      nodes.forEach(function (el) {
        renderCommonChart(el, getOptionsFromDataset(el));
      });
    });
  }
})(this);
