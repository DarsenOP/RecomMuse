export ARTIST_DIR="/home/senmrx/Documents/RecomMuse/src/artist_similarity"
export RUN_DIR="/home/senmrx/Documents/RecomMuse/src/nostalgic_recommender"

export PYTHONPATH="${ARTIST_DIR}:${PYTHONPATH:-}"

python3 "${RUN_DIR}/main.py"
