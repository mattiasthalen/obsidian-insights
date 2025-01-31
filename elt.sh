# Capture start time
start_time=$(date +%s)
echo "Script started at: $(date)"

# Run commands
uv run ./pipelines/northwind.py
uv run sqlmesh run prod

# Rest of the script execution
end_time=$(date +%s)
runtime=$((end_time-start_time))
echo "Script completed in ${runtime} seconds"
