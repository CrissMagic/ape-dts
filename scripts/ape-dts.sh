#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./ape-dts.sh start  [config-file] [log-directory]
  ./ape-dts.sh stop   [log-directory]
  ./ape-dts.sh status [log-directory]

Defaults:
  config-file    ./task_config.ini
  log-directory  ./logs

Relative paths are resolved from the directory containing ape-dts.sh.
The log directory contains console.log and ape-dts.pid. Application log files
are written to [runtime].log_dir from the task configuration.
EOF
}

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
BINARY_PATH="${SCRIPT_DIR}/ape-dts"
STOP_WAIT_SECS=30
PROCESS_PID=""

resolve_path() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s/%s\n' "${SCRIPT_DIR}" "$1" ;;
  esac
}

load_pid() {
  local pid_file=$1
  [[ -f "${pid_file}" ]] || return 1
  PROCESS_PID=$(<"${pid_file}")
  [[ "${PROCESS_PID}" =~ ^[0-9]+$ ]]
}

is_owned_process() {
  local executable
  executable=$(readlink -f "/proc/${PROCESS_PID}/exe" 2>/dev/null) || return 1
  [[ "${executable}" == "${BINARY_PATH}" ]]
}

acquire_control_lock() {
  local log_dir=$1

  if ! command -v flock >/dev/null 2>&1; then
    echo "required command 'flock' was not found" >&2
    exit 1
  fi
  if ! exec 9<"${log_dir}"; then
    echo "failed to open log directory for locking: ${log_dir}" >&2
    exit 1
  fi
  if ! flock -n 9; then
    echo "another start or stop command is already running for log directory: ${log_dir}" >&2
    exit 1
  fi
}

start() {
  if (( $# > 2 )); then
    usage >&2
    exit 2
  fi

  local config_path log_dir console_log pid_file status
  config_path=$(resolve_path "${1:-task_config.ini}")
  log_dir=$(resolve_path "${2:-logs}")
  console_log="${log_dir}/console.log"
  pid_file="${log_dir}/ape-dts.pid"

  if [[ ! -x "${BINARY_PATH}" ]]; then
    echo "ape-dts executable not found or not executable: ${BINARY_PATH}" >&2
    exit 1
  fi
  if [[ ! -r "${config_path}" ]]; then
    echo "task configuration not found or not readable: ${config_path}" >&2
    exit 1
  fi

  mkdir -p "${log_dir}"
  acquire_control_lock "${log_dir}"
  if load_pid "${pid_file}"; then
    if kill -0 "${PROCESS_PID}" 2>/dev/null; then
      if is_owned_process; then
        echo "ape-dts is already running, pid: ${PROCESS_PID}" >&2
      else
        echo "pid file points to another process; inspect before removing: ${pid_file}" >&2
      fi
      exit 1
    fi
  fi
  rm -f -- "${pid_file}"

  cd "${SCRIPT_DIR}"
  nohup "${BINARY_PATH}" --config "${config_path}" 9<&- >>"${console_log}" 2>&1 </dev/null &
  PROCESS_PID=$!
  printf '%s\n' "${PROCESS_PID}" >"${pid_file}"

  sleep 1
  if ! kill -0 "${PROCESS_PID}" 2>/dev/null; then
    status=0
    wait "${PROCESS_PID}" || status=$?
    rm -f -- "${pid_file}"
    if (( status == 0 )); then
      status=1
    fi
    echo "ape-dts failed to start; check ${console_log}" >&2
    exit "${status}"
  fi

  echo "ape-dts started, pid: ${PROCESS_PID}"
  echo "console log: ${console_log}"
  echo "application logs follow [runtime].log_dir in ${config_path}"
}

stop() {
  if (( $# > 1 )); then
    usage >&2
    exit 2
  fi

  local log_dir pid_file
  log_dir=$(resolve_path "${1:-logs}")
  pid_file="${log_dir}/ape-dts.pid"

  if [[ ! -e "${log_dir}" ]]; then
    echo "ape-dts is not running"
    return
  fi
  if [[ ! -d "${log_dir}" ]]; then
    echo "log directory is not a directory: ${log_dir}" >&2
    exit 1
  fi
  acquire_control_lock "${log_dir}"

  if ! load_pid "${pid_file}" || ! kill -0 "${PROCESS_PID}" 2>/dev/null; then
    rm -f -- "${pid_file}"
    echo "ape-dts is not running"
    return
  fi
  if ! is_owned_process; then
    echo "pid file points to another process; refusing to stop pid ${PROCESS_PID}" >&2
    exit 1
  fi

  kill -INT "${PROCESS_PID}"
  for ((second = 0; second < STOP_WAIT_SECS; second++)); do
    if ! kill -0 "${PROCESS_PID}" 2>/dev/null; then
      rm -f -- "${pid_file}"
      echo "ape-dts stopped, pid: ${PROCESS_PID}"
      return
    fi
    sleep 1
  done

  echo "ape-dts did not stop within ${STOP_WAIT_SECS}s; SIGKILL was not sent" >&2
  exit 1
}

status() {
  if (( $# > 1 )); then
    usage >&2
    exit 2
  fi

  local log_dir pid_file
  log_dir=$(resolve_path "${1:-logs}")
  pid_file="${log_dir}/ape-dts.pid"

  if ! load_pid "${pid_file}" || ! kill -0 "${PROCESS_PID}" 2>/dev/null; then
    echo "ape-dts is not running"
    exit 3
  fi
  if ! is_owned_process; then
    echo "pid file points to another process: ${PROCESS_PID}" >&2
    exit 1
  fi

  echo "ape-dts is running, pid: ${PROCESS_PID}"
}

command=${1:-}
case "${command}" in
  start)
    shift
    start "$@"
    ;;
  stop)
    shift
    stop "$@"
    ;;
  status)
    shift
    status "$@"
    ;;
  -h | --help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
