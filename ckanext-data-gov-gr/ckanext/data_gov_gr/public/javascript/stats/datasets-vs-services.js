document.addEventListener('DOMContentLoaded', function() {
    const chartContainer = document.getElementById('datasets-vs-services-pie-chart');
    if (!chartContainer) return;
    const chart = echarts.init(chartContainer);

    const table = document.querySelector('#stats-datasets-vs-services table');
    if (!table) return;

    const rows = Array.from(table.querySelectorAll('tbody tr'));
    const data = rows.map(row => ({
        name: row.cells[0].textContent.trim(),
        value: parseInt(row.cells[1].textContent.trim(), 10)
    }));

    function draw() {
        const isMobile = window.innerWidth < 768;
        const option = {
            tooltip: { trigger: 'item', formatter: ({name, value, percent}) => `${name}: ${value} (${percent}%)` },
            legend: { orient: 'horizontal', bottom: 0 },
            series: [{
                type: 'pie',
                radius: ['40%', '70%'],
                avoidLabelOverlap: false,
                label: { show: false },
                emphasis: { label: { show: true, fontSize: 14, fontWeight: 'bold' } },
                labelLine: { show: false },
                data: data
            }]
        };
        chartContainer.style.height = isMobile ? '420px' : '380px';
        chart.setOption(option);
        chart.resize();
    }

    draw();
    window.addEventListener('resize', draw);

    const toggleBtn = document.getElementById('toggleDatasetsServicesView');
    const tableView = document.getElementById('datasets-services-table-view');
    let showingChart = true;
    if (toggleBtn && tableView) {
        toggleBtn.addEventListener('click', function() {
            if (showingChart) {
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
            showingChart = !showingChart;
        });
    }
});
