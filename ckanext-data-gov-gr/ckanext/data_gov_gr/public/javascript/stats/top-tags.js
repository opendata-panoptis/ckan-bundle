document.addEventListener('DOMContentLoaded', function() {
    const chartContainer = document.getElementById('top-tags-bar-chart');
    const table = document.querySelector('#stats-top-tags table');
    const toggleBtn = document.getElementById('toggleTopTagsView');
    const tableView = document.getElementById('top-tags-table-view');

    if (!chartContainer || !table || !toggleBtn || !tableView) {
        return;
    }

    const rows = Array.from(table.querySelectorAll('tbody tr'));

    if (!rows.length) {
        return;
    }

    const chart = echarts.init(chartContainer);

    const dataset = rows.map(row => ({
        name: row.cells[0].textContent.trim(),
        value: parseInt(row.cells[1].textContent.trim(), 10)
    }));

    function updateChartLayout() {
        const isMobile = window.innerWidth < 768;
        const height = Math.max(isMobile ? 400 : 450, dataset.length * 45);
        chartContainer.style.height = `${height}px`;

        const option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'
                }
            },
            grid: {
                left: '3%',
                right: '5%',
                bottom: '5%',
                top: '5%',
                containLabel: true
            },
            xAxis: {
                type: 'value',
                min: 0,
                splitLine: {
                    show: true,
                    lineStyle: {
                        type: 'dashed',
                        opacity: 0.3
                    }
                }
            },
            yAxis: {
                type: 'category',
                inverse: true,
                data: dataset.map(item => item.name),
                axisLabel: {
                    interval: 0,
                    formatter: value => value.length > 40 ? `${value.slice(0, 37)}...` : value
                }
            },
            series: [{
                type: 'bar',
                data: dataset.map(item => item.value),
                label: {
                    show: true,
                    position: 'right'
                },
                itemStyle: {
                    color: '#2ca02c'
                }
            }]
        };

        chart.setOption(option);
        chart.resize();
    }

    updateChartLayout();

    let showingChart = true;

    toggleBtn.addEventListener('click', function() {
        if (showingChart) {
            chartContainer.style.display = 'none';
            tableView.style.display = 'block';
            const chartLabel = toggleBtn.dataset.chartLabel || 'Switch to Chart';
            toggleBtn.innerHTML = '<i class="fa fa-chart-bar"></i> ' + chartLabel;
        } else {
            chartContainer.style.display = 'block';
            tableView.style.display = 'none';
            const tableLabel = toggleBtn.dataset.tableLabel || 'Switch to Table';
            toggleBtn.innerHTML = '<i class="fa fa-table"></i> ' + tableLabel;
            chart.resize();
        }
        showingChart = !showingChart;
    });

    window.addEventListener('resize', function() {
        chart.resize();
        updateChartLayout();
    });

    function checkVisibility() {
        if (!showingChart) {
            chartContainer.style.display = 'none';
            return;
        }

        const hash = window.location.hash;
        const path = window.location.pathname;
        const isSectionActive = hash === '#stats-top-tags' || path.includes('/stats/top-tags');
        chartContainer.style.display = isSectionActive ? 'block' : 'none';

        if (isSectionActive) {
            chart.resize();
        }
    }

    checkVisibility();
    window.addEventListener('hashchange', checkVisibility);
});
