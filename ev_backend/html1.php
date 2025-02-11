<?php
/* Template Name: Email Verification List */
get_header();

// Fetch data dynamically
$output_file_name = get_query_var('output_file_name', 'Email Verification Report');
$output_file_url = get_query_var('output_file_url', '#');
$deliverable_count = get_query_var('deliverable_count', 0);
$undeliverable_count = get_query_var('undeliverable_count', 0);
$risky_count = get_query_var('risky_count', 0);
$unknown_count = get_query_var('unknown_count', 0);
$spamBlock_count = get_query_var('spamBlock_count', 0);
$tempLimited_count = get_query_var('tempLimited_count', 0);
$total = $deliverable_count + $undeliverable_count + $risky_count + $unknown_count + $spamBlock_count + $tempLimited_count;
?>

<body class="bg-gray-100 p-6">
    <div class="max-w-3xl mx-auto bg-white p-6 rounded-lg shadow-lg">
        <!-- Header -->
        <div class="flex flex-wrap justify-between items-center border-b pb-4 mb-4 gap-4">
            <div>
                <h2 class="text-xl font-semibold text-gray-800"><?php echo esc_html($output_file_name); ?></h2>
            </div>
            <div class="flex gap-4">
                <a href="<?php echo esc_url(home_url('/bulk-email-verify/')); ?>" class="text-red-500 px-4 py-2 rounded-lg hover:text-red-600 flex items-center">
                    <span class="mr-2">&larr;</span> Back to Lists
                </a>
                <a href="<?php echo esc_url($output_file_url); ?>" class="bg-red-500 text-white px-4 py-2 rounded-lg hover:bg-red-600">Download results</a>
            </div>
        </div>

        <!-- List Summary -->
        <h3 class="text-lg font-medium text-gray-700 mb-4">List Summary</h3>
        <div class="flex flex-wrap sm:flex-nowrap items-center gap-6">
            <div class="w-40 h-40 mx-auto sm:mx-0">
                <canvas id="pieChart"></canvas>
            </div>
            <div class="grid grid-cols-1 gap-4 w-full">
                <?php 
                $categories = [
                    'Valid' => ['count' => $deliverable_count, 'color' => 'green-600', 'desc' => 'The recipient is valid and email can be delivered.'],
                    'Invalid' => ['count' => $undeliverable_count, 'color' => 'red-600', 'desc' => 'The recipient is invalid and email cannot be delivered.'],
                    'Catch All' => ['count' => $risky_count, 'color' => 'yellow-500', 'desc' => 'The email address is valid but the deliverability is uncertain.'],
                    'Unknown' => ['count' => $unknown_count, 'color' => 'gray-400', 'desc' => 'The address format is correct, but the recipient’s server is unresponsive.'],
                    'Spam Block' => ['count' => $spamBlock_count, 'color' => 'purple-500', 'desc' => 'The email was blocked due to spam filters.'],
                    'Temporary Limited' => ['count' => $tempLimited_count, 'color' => 'blue-500', 'desc' => 'The recipient’s server temporarily rejected the email.']
                ];
                
                foreach ($categories as $label => $data) {
                    $percent = $total > 0 ? round(($data['count'] / $total) * 100, 1) : 0;
                    echo "<div class='flex items-center gap-2 text-{$data['color']} font-medium'>
                            <span class='w-4 h-4 bg-{$data['color']} rounded-full'></span> $label
                            <span class='text-gray-500 ml-auto'>{$data['count']}</span>
                            <span class='text-gray-500'>{$percent}%</span>
                          </div>
                          <p class='text-sm text-gray-500'>{$data['desc']}</p>";
                }
                ?>
            </div>
        </div>
    </div>

    <script>
    document.addEventListener("DOMContentLoaded", function() {
        const ctx = document.getElementById("pieChart").getContext("2d");
        new Chart(ctx, {
            type: "doughnut",
            data: {
                labels: ["Deliverable", "Undeliverable", "Catch All", "Unknown", "Spam Block", "Temporary Limited"],
                datasets: [{
                    data: [
                        <?php echo esc_js($deliverable_count); ?>,
                        <?php echo esc_js($undeliverable_count); ?>,
                        <?php echo esc_js($risky_count); ?>,
                        <?php echo esc_js($unknown_count); ?>,
                        <?php echo esc_js($spamBlock_count); ?>,
                        <?php echo esc_js($tempLimited_count); ?>
                    ],
                    backgroundColor: ["#16A34A", "#DC2626", "#EAB308", "#9CA3AF", "#9333EA", "#3B82F6"],
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                }
            }
        });
    });
    </script>
</body>

<?php get_footer(); ?>