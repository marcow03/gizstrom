$(async () => {
    // Fetch data from both endpoints
    try {
        const [histRes, foreRes] = await Promise.allSettled([
            fetch('/power-generation/historical/').then(r => {
                if (!r.ok) throw new Error(`Historical data fetch failed with status ${r.status}`);
                return r.json();
            }),
            fetch('/power-generation/forecast/').then(r => {
                if (!r.ok) throw new Error(`Forecast data fetch failed with status ${r.status}`);
                return r.json();
            })
        ]);

        // Check results and handle failures
        const historicalData = histRes.status === "fulfilled" ? histRes.value : [];
        const forecastData = foreRes.status === "fulfilled" ? foreRes.value : [];

        // Format the data for Chart.js
        // Mapping time to date and the specific kWh keys to a unified format
        // Only display the last 14 days of historical data
        const measurementData = historicalData.map(d => ({
            date: d.time.split('T')[0],
            kwh: d.power_generation_kwh
        })).slice(-14);

        const predictedData = forecastData.map(d => ({
            date: d.time.split('T')[0],
            kwh: d.pred_power_generation_kwh
        }));

        const labels = [...measurementData.map(d => d.date), ...predictedData.map(d => d.date)];

        // Initialize Chart
        const ctx = document.getElementById('energyChart').getContext('2d');
        new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Past Generation',
                        data: measurementData.map(d => d.kwh),
                        borderColor: 'hsl(240 6% 10%)',
                        backgroundColor: 'rgba(24, 24, 27, 0.05)',
                        borderWidth: 2,
                        tension: 0.3,
                        fill: true
                    },
                    {
                        label: 'Predicted Generation',
                        data: [
                            ...Array(Math.max(0, measurementData.length - 1)).fill(null),
                            measurementData.length > 0 ? measurementData[measurementData.length - 1].kwh : null,
                            ...predictedData.map(d => d.kwh)
                        ],
                        borderColor: '#22c55e',
                        backgroundColor: 'rgba(34, 197, 94, 0.05)',
                        borderWidth: 2,
                        borderDash: [5, 5],
                        tension: 0.3,
                        fill: true
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: true, labels: { usePointStyle: true, pointStyle: "line" } } },
                scales: {
                    y: { beginAtZero: true, grid: { color: 'hsl(240 5.9% 90%)' } },
                    x: { grid: { display: false } }
                }
            }
        });
    } catch (err) {
        console.error("Error loading energy data:", err);
    }

    // UI Logic (Modal & File Upload)
    const $modal = $("#uploadModal");
    const $fileInput = $("#csvInput");
    const $dropZone = $("#dropZone");
    const $fileInfo = $("#fileInfo");
    const $fileName = $("#fileName");

    $("#openModal").click(() => $modal.removeClass("hidden").addClass("flex"));

    const closeModal = () => {
        $modal.addClass("hidden").removeClass("flex");
        resetUpload();
    };

    $("#closeModal, #uploadModal").click((e) => {
        if (e.target.id === "uploadModal" || e.target.id === "closeModal") closeModal();
    });

    // Prevent event bubbling on the modal content itself
    $(".modal-content").click((e) => e.stopPropagation());

    // Click Dropzone to trigger file input
    $dropZone.on('click', function (e) {
        $fileInput.trigger('click');
    });

    // Stop click propagation on input to prevent loop
    $fileInput.on('click', function (e) {
        e.stopPropagation();
    });

    // Handle Drag & Drop events
    $dropZone.on("dragover dragenter", function (e) {
        e.preventDefault();
        e.stopPropagation();
        $(this).addClass("bg-slate-100 border-primary");
    });

    $dropZone.on("dragleave drop", function (e) {
        e.preventDefault();
        e.stopPropagation();
        $(this).removeClass("bg-slate-100 border-primary");
    });

    $dropZone.on("drop", function (e) {
        const files = e.originalEvent.dataTransfer.files;
        if (files.length) {
            $fileInput[0].files = files;
            handleFileUI(files[0]);
        }
    });

    $fileInput.on("change", function () {
        if (this.files && this.files[0]) {
            handleFileUI(this.files[0]);
        }
    });

    function handleFileUI(file) {
        $fileName.text(file.name);
        $dropZone.addClass("hidden");
        $fileInfo.removeClass("hidden").addClass("flex");
    }

    $("#removeFile").on('click', function (e) {
        e.preventDefault();
        e.stopPropagation();
        resetUpload();
    });

    function resetUpload() {
        $fileInput.val("");
        $dropZone.removeClass("hidden");
        $fileInfo.addClass("hidden").removeClass("flex");
    }

    $("#processBtn").click(() => {
        const file = $fileInput[0].files[0];
        if (file) {
            const formData = new FormData();
            formData.append("file", file);

            fetch("/upload/", {
                method: "POST",
                body: formData
            }).then(response => response.json())
                .then(() => closeModal())
                .catch(() => alert("Failed to upload file."));
        } else {
            alert("Please select a file first.");
        }
    });
});

tailwind.config = {
    theme: {
        extend: {
            colors: {
                border: "hsl(240 6% 90%)",
                background: "hsl(0 0% 100%)",
                foreground: "hsl(240 10% 4%)",
                muted: "hsl(240 4% 45%)",
                primary: {
                    DEFAULT: "hsl(240 6% 10%)",
                    foreground: "hsl(0 0% 98%)",
                },
                card: "hsl(0 0% 100%)",
            },
            borderRadius: {
                lg: "0.5rem",
                md: "calc(0.5rem - 2px)",
                sm: "calc(0.5rem - 4px)",
            },
        }
    }
}
