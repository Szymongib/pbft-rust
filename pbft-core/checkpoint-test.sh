# TODO: write it properly as an e2e test.

REPLICA=${1:-0}

# Loop 15 times
for i in {1..15}
do
  curl "localhost:1000${REPLICA}/api/v1/kv" -H "Content-Type: application/json" -d '{"key":"key-'$i'", "value":"value-'$i'"}' -v
done


# TODO: this should be more automated but hey ¯\_(ツ)_/¯
echo "Done! Now check the logs for the checkpointing and message discarding!"
