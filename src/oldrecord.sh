#!/bin/bash
# =============================================================================
# TTL Handling Test Suite
#
# Three independent tests for TWCS / TTL verification.
# Each test inserts a single record with a back-dated USING TIMESTAMP so that
# the row appears older than "now", and a TTL calculated to expire in the
# future relative to now (not relative to the write time).
#
# Intended to be scheduled 6 hours apart via cron, e.g.:
#   0  0 * * *  /path/to/oldrecord.sh test1
#   0  6 * * *  /path/to/oldrecord.sh test2
#   0 12 * * *  /path/to/oldrecord.sh test3
#
# Usage:  ./oldrecord.sh <test1|test2|test3>
#         ./oldrecord.sh          # runs all three
# =============================================================================

KEYSPACE="romain"
TABLE="ads_twcs"
OWNEDBY="Romain"

# Optional: copy the generated CQL to a node and run it there over SSH.
# Set these in the environment (nothing is hard-coded), then uncomment the
# scp/ssh lines in run_test.
SSH_USER="${SSH_USER:-}"
SSH_HOST="${SSH_HOST:-}"
SSH_KEY="${SSH_KEY:-}"

# ---------------------------------------------------------------------------
# run_test  ID  OFFSET_SEC  FUTURE_SEC
#
#   ID          record id / test number
#   OFFSET_SEC  how far in the past the write timestamp is back-dated (seconds)
#   FUTURE_SEC  how far in the future from *now* the row should expire (seconds)
#
# Derived values:
#   WRITE_TS = (now - OFFSET_SEC) * 1_000_000   [µs, as required by CQL]
#   TTL      = OFFSET_SEC + FUTURE_SEC           [s; anchored to write time]
# ---------------------------------------------------------------------------
run_test () {
    local ID=$1
    local OFFSET_SEC=$2
    local FUTURE_SEC=$3

    local NOW_SEC
    NOW_SEC=$(date +%s)

    local WRITE_TS=$(( (NOW_SEC - OFFSET_SEC) * 1000000 ))
    local TTL=$(( OFFSET_SEC + FUTURE_SEC ))

    local TIME_VAL
    TIME_VAL="$(date -u -d "@$((NOW_SEC - OFFSET_SEC))" '+%Y-%m-%d %H:%M:%S+0000')"
    local CURRENT_VAL
    CURRENT_VAL="$(date -u '+%Y-%m-%d %H:%M:%S+0000')"
    local NOTES="TTL-test${ID}: written $((OFFSET_SEC/3600))h ago, expires in $((FUTURE_SEC/3600))h (write_ts=${WRITE_TS})"

    echo "// WRITE_TS (µs): $WRITE_TS" | tee -a ttl-$ID.cql
    echo "// TTL      (s):  $TTL  (~$(( TTL / 3600 ))h)" | tee -a ttl-$ID.cql
    echo ""
    echo "INSERT INTO ${KEYSPACE}.${TABLE} (id, ownedby, ticket, time, notes, updatedat)" | tee -a ttl-$ID.cql
    echo "VALUES (${ID}, '${OWNEDBY}', $((RANDOM % 100000 + 40000)), '${TIME_VAL}', '${NOTES}', '${CURRENT_VAL}')" | tee -a ttl-$ID.cql
    echo "USING TIMESTAMP ${WRITE_TS} AND TTL ${TTL};" | tee -a ttl-$ID.cql

    #scp -i "$SSH_KEY" ttl-$ID.cql "${SSH_USER}@${SSH_HOST}:~"
    #ssh -i "$SSH_KEY" "${SSH_USER}@${SSH_HOST}" -- "cqlsh -f ttl-$ID.cql"
}

# ---------------------------------------------------------------------------
# Test declarations — only the numbers live here
#
#            ID   back-date        expiry from now
#                 (OFFSET_SEC)     (FUTURE_SEC)
# ---------------------------------------------------------------------------
test1 () { run_test 1 $(( 3 * 3600))  $((  24 * 3600)); }   # 3h ago  → +24h
test2 () { run_test 2 $((24 * 3600))  $((  24 * 3600)); }   # 24h ago → +24h
test3 () { run_test 3 $(( 6 * 86400)) $((   7 * 86400)); }  # 6d ago  → +7d
test4 () { run_test 4 $(( 6 * 86400)) $((   0)); }  # 6d ago  → +7d

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
case "${1:-all}" in
    test1) echo "// === TEST 1: back-date 3h,  TTL expires in 24h ===" | tee ttl-1.cql && test1 ;;
    test2) echo "// === TEST 2: back-date 24h, TTL expires in 24h ===" | tee ttl-2.cql && test2 ;;
    test3) echo "// === TEST 3: back-date 6d,  TTL expires in 7d  ===" | tee ttl-3.cql && test3 ;;
    test4) echo "// === TEST 3: back-date 6d,  TTL without expiration 0  ===" | tee ttl-4.cql && test4 ;;
    all)
        echo "=== TEST 1: back-date 3h,  TTL expires in 24h ===" && test1; echo ""
        echo "=== TEST 2: back-date 24h, TTL expires in 24h ===" && test2; echo ""
        echo "=== TEST 3: back-date 6d,  TTL expires in 7d  ===" && test3; echo ""
        echo "=== TEST 4: back-date 6d,  TTL NO expiry  ===" && test4
        ;;
    *)
        echo "Usage: $0 <test1|test2|test3|all>" >&2
        exit 1
        ;;
esac
