document.addEventListener('DOMContentLoaded', function() {
    const chartContainer = document.getElementById('theme-pie-chart');
    const chart = echarts.init(chartContainer);

    // Παίρνουμε τα δεδομένα από τον πίνακα
    const table = document.querySelector('#stats-datasets-per-theme table');
    const data = Array.from(table.querySelectorAll('tbody tr')).map(row => ({
        code: row.cells[0].textContent.trim(),
        label: row.cells[1].textContent.trim(),
        count: parseInt(row.cells[2].textContent.trim(), 10)
    }));

    function updateChartLayout() {
        const isMobile = window.innerWidth < 768; // Bootstrap's md breakpoint

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

        // Προσαρμογή ύψους του container
        const legendHeight = (data.length * 25) + 50;
        chartContainer.style.height = isMobile ?
            `${Math.max(600, legendHeight + 350)}px` :
            '500px';

        chart.resize();
        chart.setOption(option);
    }

    // Αρχική ρύθμιση
    updateChartLayout();

    // Toggle functionality
    const toggleBtn = document.getElementById('toggleView');
    const tableView = document.getElementById('theme-table-view');
    let showingPie = true;

    toggleBtn.addEventListener('click', function() {
        if (showingPie) {
            chartContainer.style.display = 'none';
            tableView.style.display = 'block';
            toggleBtn.innerHTML = '<i class="fa fa-chart-pie"></i> ' + 'Switch to Chart';
        } else {
            chartContainer.style.display = 'block';
            tableView.style.display = 'none';
            toggleBtn.innerHTML = '<i class="fa fa-table"></i> ' + 'Switch to Table';
            chart.resize();
        }
        showingPie = !showingPie;
    });

    // Responsive behavior
    window.addEventListener('resize', function() {
        chart.resize();
        updateChartLayout();
    });
});

// Απόκρυψη και εμφάνιση του div του chart των στατιστικών datasets per theme
document.addEventListener('DOMContentLoaded', function() {
    checkThemeVisibility();
    window.addEventListener('hashchange', checkThemeVisibility);

    function checkThemeVisibility() {
        const path = window.location.pathname;
        const pieChart = document.getElementById('theme-pie-chart');

        // Έλεγχος αν είμαστε στο theme section
        const isThemeSection = path.includes('/stats/datasets-by-theme');

        // Ρύθμιση ορατότητας
        pieChart.style.display = isThemeSection ? 'block' : 'none';
    }
});
