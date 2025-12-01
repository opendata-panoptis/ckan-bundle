document.addEventListener('DOMContentLoaded', function() {
    const chartContainer = document.getElementById('publisher-pie-chart');
    const chart = echarts.init(chartContainer);

    // Get data from the table
    const table = document.querySelector('#stats-organizations-per-type table');
    const data = Array.from(table.querySelectorAll('tbody tr')).map(row => ({
        code: row.cells[0].textContent.trim(),
        label: row.cells[1].textContent.trim(),
        count: parseInt(row.cells[2].textContent.trim(), 10)
    }));

    function updateChartLayout() {
        const isMobile = window.innerWidth < 768;

        const option = {
            title: {
                text: '',
                left: 'center'
            },
            tooltip: {
                trigger: 'item',
                formatter: ({name, value, percent}) => `${name}: ${value} (${percent}%)`
            },
            legend: {
                orient: 'vertical',
                left: isMobile ? 'center' : 'right',
                top: isMobile ? '70%' : 'middle',
                itemWidth: 10,
                itemHeight: 10,
                textStyle: {
                    fontSize: 12,
                    padding: [3, 0, 0, 0]
                },
                formatter: function(name) {
                    if (name.length > 40) {
                        return name.substring(0, 37) + '...';
                    }
                    return name;
                }
            },
            series: [
                {
                    type: 'pie',
                    radius: isMobile ? '55%' : '65%',
                    center: isMobile ? ['50%', '35%'] : ['35%', '50%'],
                    label: {
                        show: false
                    },
                    data: data.map(item => ({
                        name: item.label,
                        value: item.count
                    })),
                    emphasis: {
                        itemStyle: {
                            shadowBlur: 10,
                            shadowOffsetX: 0,
                            shadowColor: 'rgba(0, 0, 0, 0.5)'
                        }
                    }
                }
            ]
        };

        const legendHeight = (data.length * 25) + 50;
        chartContainer.style.height = isMobile ?
            `${Math.max(600, legendHeight + 350)}px` :
            '500px';

        chart.resize();
        chart.setOption(option);
    }

    updateChartLayout();

    // Toggle functionality
    const toggleBtn = document.getElementById('toggleOrgView');
    const tableView = document.getElementById('publisher-table-view');
    let showingPie = true;

    toggleBtn.addEventListener('click', function() {
        if (showingPie) {
            chartContainer.style.display = 'none';
            tableView.style.display = 'block';
            const chartLabel = toggleBtn.dataset.chartLabel || 'Switch to Chart';
            toggleBtn.innerHTML = '<i class="fa fa-chart-pie"></i> ' + chartLabel;
        } else {
            chartContainer.style.display = 'block';
            tableView.style.display = 'none';
            const tableLabel = toggleBtn.dataset.tableLabel || 'Switch to Table';
            toggleBtn.innerHTML = '<i class="fa fa-table"></i> ' + tableLabel;
            chart.resize();
        }
        showingPie = !showingPie;
    });

    window.addEventListener('resize', function() {
        chart.resize();
        updateChartLayout();
    });
});

// Toggle chart visibility based on section
document.addEventListener('DOMContentLoaded', function() {
    checkPublisherVisibility();
    window.addEventListener('hashchange', checkPublisherVisibility);

    function checkPublisherVisibility() {
        const path = window.location.pathname;
        const pieChart = document.getElementById('publisher-pie-chart');
        const isPublisherSection = path.includes('/stats/organizations-by-publisher-type')
            || path.includes('/stats/stats-organizations-per-type');
        pieChart.style.display = isPublisherSection ? 'block' : 'none';
    }
});
