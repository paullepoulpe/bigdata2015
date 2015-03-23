#!/bin/bash


#INPUT="/datasets/hw1_input_gz/small"
#INPUT="/datasets/hw1_input_gz/big"
#INPUT="/datasets/hw1_input_gz/big_text_only"
#OUTPUT="/user/engels/output-$(basename $INPUT)"
INPUT=$1
OUTPUT=$2

error(){
    echo "[run_hw1.sh - ERROR] $1"
}

info(){
    echo "[run_hw1.sh - INFO] $1"
}

fail() {
    error "$1"
    exit 1
}

# Package current code
info "Compiling project"
mvn package || fail "Could not compile code"

# Remove output folder
info "Removing output folder"
hadoop fs -rm -r "$OUTPUT" || error "Failed to remove output folder"

# Run the code
info "Running project on input=$INPUT and output=$OUTPUT"
hadoop jar target/homework1-1.0.jar Homework1 "$INPUT" "$OUTPUT" || fail "Hadoop was not able to run"
