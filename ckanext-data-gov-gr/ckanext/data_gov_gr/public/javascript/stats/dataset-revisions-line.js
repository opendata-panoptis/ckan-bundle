document.addEventListener('DOMContentLoaded', function() {
    const chartContainer = document.getElementById('dataset-revisions-line-chart');
    const table = document.querySelector('#stats-dataset-revisions table');
    const toggleBtn = document.getElementById('toggleDatasetRevisionsView');
    const tableView = document.getElementById('dataset-revisions-table-view');

    if (!chartContainer || !table || !toggleBtn || !tableView) {
        return;
    }

    const chart = echarts.init(chartContainer);

    const rows = Array.from(table.querySelectorAll('tbody tr'));
    if (!rows.length) {
        return;
    }

    const dataset = rows.map(row => ({
        date: new Date(row.cells[0].querySelector('time').getAttribute('datetime')),
        total: parseInt(row.cells[1].textContent.trim(), 10),
        created: parseInt(row.cells[2].textContent.trim(), 10)
    }));

    function updateChartLayout() {
        const isMobile = window.innerWidth < 768;

        chartContainer.style.height = isMobile ? '400px' : '500px';

        const totalSeriesName = chartContainer.dataset.seriesTotal || 'All Dataset Revisions';
        const newSeriesName = chartContainer.dataset.seriesNew || 'New Datasets';
        const option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'line'
                },
                formatter: params => {
                    const [totalPoint, createdPoint] = params;
                    const date = new Date(totalPoint.value[0]);
                    const formattedDate = date.toLocaleDateString(document.documentElement.lang || 'el', {
                        year: 'numeric',
                        month: 'long',
                        day: 'numeric'
                    });
                    const totalLabel = chartContainer.dataset.seriesTotal || 'All Dataset Revisions';
                    const newLabel = chartContainer.dataset.seriesNew || 'New Datasets';
                    return [
                        formattedDate,
                        `${totalLabel}: ${totalPoint.value[1]}`,
                        `${newLabel}: ${createdPoint.value[1]}`
                    ].join('<br/>');
                }
            },
            legend: {
                data: [totalSeriesName, newSeriesName],
                top: 0
            },
            grid: {
                left: '8%',
                right: '5%',
                bottom: '12%',
                top: '12%',
                containLabel: true
            },
            xAxis: {
                type: 'time',
                axisLabel: {
                    formatter: value => {
                        const date = new Date(value);
                        return date.toLocaleDateString('el', {
                            year: 'numeric',
                            month: 'short'
                        });
                    },
                    rotate: 45
                }
            },
            yAxis: {
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
            series: [
                {
                    name: totalSeriesName,
                    type: 'line',
                    smooth: true,
                    symbol: 'circle',
                    symbolSize: 6,
                    data: dataset.map(item => [item.date, item.total]),
                    lineStyle: {
                        width: 3
                    }
                },
                {
                    name: newSeriesName,
                    type: 'line',
                    smooth: true,
                    symbol: 'circle',
                    symbolSize: 6,
                    data: dataset.map(item => [item.date, item.created]),
                    lineStyle: {
                        width: 3
                    }
                }
            ]
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
            toggleBtn.innerHTML = '<i class="fa fa-chart-line"></i> ' + chartLabel;
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
        const isSectionActive = !hash || hash === '#dataset-revisions' || hash === '#stats-dataset-revisions';

        chartContainer.style.display = isSectionActive ? 'block' : 'none';

        if (isSectionActive) {
            chart.resize();
        }
    }

    checkVisibility();
    window.addEventListener('hashchange', checkVisibility);
});
