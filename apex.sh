#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  echo "warning: no active virtualenv detected (VIRTUAL_ENV unset)"
  echo "         the script will still run, but imports may fail."
  echo
fi

prompt_choice() {
  local prompt="$1"; shift
  local options=("$@")
  local i=1
  echo "$prompt" >&2
  for opt in "${options[@]}"; do
    printf "  %d) %s\n" "$i" "$opt" >&2
    i=$((i + 1))
  done
  local choice
  while true; do
    read -r -p "> " choice
    if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= ${#options[@]} )); then
      echo "${options[$((choice - 1))]}"
      return 0
    fi
    echo "invalid choice, try again" >&2
  done
}

# pick_env_name PRESET... : choose one of the presets or type a custom name.
pick_env_name() {
  local presets=("$@" "custom...")
  local pick
  pick="$(prompt_choice "Env name:" "${presets[@]}")"
  if [[ "$pick" == "custom..." ]]; then
    local custom
    read -r -p "Enter env name: " custom
    if [[ -z "$custom" ]]; then
      echo "env name cannot be empty" >&2
      exit 1
    fi
    echo "$custom"
  else
    echo "$pick"
  fi
}

confirm_and_run() {
  local cmd_str="$1"; shift
  echo
  echo "Command:"
  echo "  $cmd_str"
  echo
  read -r -p "Run this? [Y/n] " yn
  case "${yn:-y}" in
    [nN]|[nN][oO]) echo "aborted." ;;
    *)
      echo "Now running: $cmd_str"
      "$@"
      ;;
  esac
}

# Env names starting with "chaos-" trigger the 30s kill -9 window in the
# workflow, so they only appear under the Chaos variation.
STANDARD_ENVS=("ephemeral-test-01" "ephemeral-test-02" "ephemeral-test-03")
FAIL_ENVS=("fail-app-01" "fail-app-02")
CHAOS_ENVS=("chaos-test-01" "chaos-test-02")

# Target: $APEX_TARGET if set, otherwise ask. Every command gets --target, and
# the worker for that target must be running (python worker.py --target ...).
target="${APEX_TARGET:-}"
if [[ -z "$target" ]]; then
  target="$(prompt_choice "Target:" "aws" "local" "sim")"
fi
echo "Target: $target" >&2
T=(--target "$target")

action="$(prompt_choice "What do you want to run?" "Provision" "Approve/Reject a plan" "Swap app version" "Teardown")"

case "$action" in
  Provision)
    variation="$(prompt_choice "Variation:" "Standard" "Fail app deploy" "Chaos (kill -9 window)" "Require approval")"
    case "$variation" in
      "Require approval")
        env="$(pick_env_name "${STANDARD_ENVS[@]}")"
        args=("$env" "--require-approval")
        ;;
      "Chaos (kill -9 window)")
        env="$(pick_env_name "${CHAOS_ENVS[@]}")"
        args=("$env")
        ;;
      "Fail app deploy")
        env="$(pick_env_name "${FAIL_ENVS[@]}")"
        args=("$env" "--fail-app-deploy")
        ;;
      *)
        env="$(pick_env_name "${STANDARD_ENVS[@]}")"
        args=("$env")
        ;;
    esac
    confirm_and_run "python starter.py ${args[*]} ${T[*]}" python starter.py "${args[@]}" "${T[@]}"
    ;;
  "Approve/Reject a plan")
    env="$(pick_env_name "${STANDARD_ENVS[@]}")"
    module="$(prompt_choice "Module:" "network" "compute" "app")"
    # Show the plan first; this fails if the module isn't running.
    python approve.py "$env" "$module" "${T[@]}" || exit 1
    echo
    decision="$(prompt_choice "Decision:" "Approve" "Reject")"
    read -r -p "Reason: " reason
    flag="--approve"
    [[ "$decision" == "Reject" ]] && flag="--reject"
    confirm_and_run "python approve.py $env $module $flag --reason \"$reason\" ${T[*]}" \
      python approve.py "$env" "$module" "$flag" --reason "$reason" "${T[@]}"
    ;;
  "Swap app version")
    env="$(pick_env_name "${STANDARD_ENVS[@]}" "${CHAOS_ENVS[@]}")"
    read -r -p "New app version [v2]: " version
    version="${version:-v2}"
    confirm_and_run "python swapper.py $env $version ${T[*]}" python swapper.py "$env" "$version" "${T[@]}"
    ;;
  Teardown)
    env="$(pick_env_name "${STANDARD_ENVS[@]}" "${FAIL_ENVS[@]}" "${CHAOS_ENVS[@]}")"
    confirm_and_run "python teardown.py $env ${T[*]}" python teardown.py "$env" "${T[@]}"
    ;;
esac
