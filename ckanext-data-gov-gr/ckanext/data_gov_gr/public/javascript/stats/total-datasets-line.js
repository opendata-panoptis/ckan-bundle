document.addEventListener('DOMContentLoaded', function() {
    const chartContainer = document.getElementById('total-datasets-line-chart');
    const chart = echarts.init(chartContainer);

    // Παίρνουμε τα δεδομένα από τον πίνακα
    const table = document.querySelector('#stats-total-datasets table');
    const data = Array.from(table.querySelectorAll('tbody tr')).map(row => ({
        date: new Date(row.cells[0].querySelector('time').getAttribute('datetime')),
        total: parseInt(row.cells[1].textContent.trim(), 10)
    }));

    function updateChartLayout() {
        const isMobile = window.innerWidth < 768;
        const seriesName = chartContainer.dataset.seriesTotal || 'Total Datasets';
        const yLabel = chartContainer.dataset.ylabel || 'Total Datasets';

        const option = {
            title: {
                text: '',
                left: 'center'
            },
            tooltip: {
                trigger: 'axis',
                formatter: function(params) {
                    const date = new Date(params[0].value[0]);
                    const formatted = date.toLocaleDateString(document.documentElement.lang || 'el', {
                        year: 'numeric',
                        month: 'long',
                        day: 'numeric'
                    });
                    const label = chartContainer.dataset.ylabel || 'Total Datasets';
                    return formatted + '<br/>' + label + ': ' + params[0].value[1];
                },
                axisPointer: {
                    type: 'line',
                    label: {
                        backgroundColor: '#6a7985'
                    }
                }
            },
            xAxis: {
                type: 'time',
                axisLabel: {
                    formatter: function(value) {
                        const date = new Date(value);
                        return date.toLocaleDateString('el', {
                            year: 'numeric',
                            month: 'short'
                        });
                    },
                    interval: 'auto',
                    rotate: 45,
                    align: 'left',
                    margin: 15
                },
                splitLine: {
                    show: true,
                    lineStyle: {
                        type: 'dashed',
                        opacity: 0.3
                    }
                },
                axisPointer: {
                    label: {
                        formatter: function(params) {
                            const date = new Date(params.value);
                            return date.toLocaleDateString('el', {
                                year: 'numeric',
                                month: 'long',
                                day: 'numeric'
                            });
                        }
                    }
                },
                minInterval: 3600 * 24 * 1000 * 30, // Ελάχιστο διάστημα 30 ημέρες
                splitNumber: 6 // Προτεινόμενος αριθμός διαστημάτων
            },
            yAxis: {
                type: 'value',
                name: yLabel,
                nameLocation: 'middle',
                nameGap: 50,
                axisLabel: {
                    formatter: '{value}'
                }
            },
            series: [{
                name: seriesName,
                type: 'line',
                smooth: true,
                symbol: 'circle',
                symbolSize: 6,
                data: data.map(item => [item.date, item.total]),
                lineStyle: {
                    width: 3
                },
                itemStyle: {
                    color: '#1f77b4'
                },
                areaStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{
                        offset: 0,
                        color: 'rgba(31, 119, 180, 0.3)'
                    }, {
                        offset: 1,
                        color: 'rgba(31, 119, 180, 0.1)'
                    }])
                }
            }],
            grid: {
                left: '10%',
                right: '10%',
                bottom: '15%',
                top: '10%',
                containLabel: true
            }
        };

        // Προσαρμογή ύψους του container
        chartContainer.style.height = isMobile ? '400px' : '500px';

        chart.setOption(option);
        chart.resize();
    }

    // Αρχική ρύθμιση
    updateChartLayout();

    // Toggle functionality
    const toggleBtn = document.getElementById('toggleTotalDatasetsView');
    const tableView = document.getElementById('total-datasets-table-view');
    let showingLine = true;

    toggleBtn.addEventListener('click', function() {
        if (showingLine) {
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
        showingLine = !showingLine;
    });

    function checkVisibility() {
        if (!showingLine) {
            chartContainer.style.display = 'none';
            return;
        }

        const hash = window.location.hash;
        const isActive = !hash || hash === '#total-datasets' || hash === '#stats-total-datasets';
        chartContainer.style.display = isActive ? 'block' : 'none';

        if (isActive) {
            chart.resize();
        }
    }

    checkVisibility();
    window.addEventListener('hashchange', checkVisibility);

    // Responsive behavior
    window.addEventListener('resize', function() {
        chart.resize();
        updateChartLayout();
    });
});
