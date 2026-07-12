// Navigation logic
document.querySelectorAll('.nav li').forEach(item => {
    item.addEventListener('click', () => {
        // Update active nav
        document.querySelectorAll('.nav li').forEach(nav => nav.classList.remove('active'));
        item.classList.add('active');

        // Show target page
        const target = item.getAttribute('data-target');
        document.querySelectorAll('.page').forEach(page => page.classList.remove('active'));
        document.getElementById(target).classList.add('active');

        if (target === 'configuration') {
            fetchConfig();
        }
    });
});

// Formatters
const formatBytes = (bytes) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

const formatUptime = (seconds) => {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    if (h > 0) return `${h}h ${m}m ${s}s`;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
};

const formatLsn = (lsn) => {
    return `0x${BigInt(lsn).toString(16).toUpperCase()}`;
};

// Sparkline history arrays
const maxSparkPoints = 20;
let rowsHistory = new Array(maxSparkPoints).fill(0);
let bytesHistory = new Array(maxSparkPoints).fill(0);
let filesHistory = new Array(maxSparkPoints).fill(0);

// Last seen values to calculate deltas
let lastRows = null;
let lastBytes = null;
let lastFiles = null;

const updateSparkline = (containerId, historyArray, newValue, lastValue) => {
    const delta = (lastValue === null) ? 0 : Math.max(0, newValue - lastValue);
    
    // Shift and push
    historyArray.shift();
    historyArray.push(delta);
    
    const container = document.getElementById(containerId);
    container.innerHTML = '';
    
    const maxVal = Math.max(...historyArray, 1); // Avoid div by zero
    
    historyArray.forEach(val => {
        const bar = document.createElement('div');
        bar.className = 'spark-bar';
        const heightPercent = (val / maxVal) * 100;
        bar.style.height = `${Math.max(5, heightPercent)}%`; // Min height 5% for visibility
        container.appendChild(bar);
    });
};

// Metrics polling
const fetchMetrics = async () => {
    try {
        const response = await fetch('/api/metrics');
        if (!response.ok) return;
        
        const data = await response.json();
        
        // Update DOM values
        document.getElementById('val-rows').innerText = data.rows_ingested.toLocaleString();
        document.getElementById('val-bytes').innerText = formatBytes(data.bytes_processed);
        document.getElementById('val-files').innerText = data.files_written.toLocaleString();
        document.getElementById('val-uptime').innerText = formatUptime(data.uptime_seconds);
        document.getElementById('val-lsn').innerText = formatLsn(data.current_lsn);
        
        // Update sparklines
        updateSparkline('spark-rows', rowsHistory, data.rows_ingested, lastRows);
        updateSparkline('spark-bytes', bytesHistory, data.bytes_processed, lastBytes);
        updateSparkline('spark-files', filesHistory, data.files_written, lastFiles);
        
        lastRows = data.rows_ingested;
        lastBytes = data.bytes_processed;
        lastFiles = data.files_written;
        
    } catch (e) {
        console.error("Failed to fetch metrics", e);
    }
};

// Configuration API
const fetchConfig = async () => {
    try {
        const response = await fetch('/api/config');
        if (response.ok) {
            const data = await response.json();
            document.getElementById('config-text').value = data.config;
            showStatus('Configuration loaded successfully', 'success');
        } else {
            showStatus('Failed to load configuration', 'error');
        }
    } catch (e) {
        showStatus('Error communicating with server', 'error');
    }
};

document.getElementById('config-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const configData = document.getElementById('config-text').value;
    
    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ config: configData })
        });
        
        if (response.ok) {
            showStatus('Configuration saved successfully. Restart daemon to apply changes.', 'success');
        } else {
            showStatus('Failed to save configuration', 'error');
        }
    } catch (e) {
        showStatus('Error communicating with server', 'error');
    }
});

document.getElementById('btn-reload').addEventListener('click', fetchConfig);

const showStatus = (msg, type) => {
    const el = document.getElementById('config-status');
    el.innerText = msg;
    el.className = `status-msg status-${type}`;
    setTimeout(() => { el.innerText = ''; }, 5000);
};

// Start polling
setInterval(fetchMetrics, 1000);
fetchMetrics();
