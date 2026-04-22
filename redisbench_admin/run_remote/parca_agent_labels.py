#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
"""Per-test parca-agent label injection for Polar Signals profiling.

When a DB node has parca-agent installed and running (e.g. via the
`enable_parca_agent` cloud-init on the oss-standalone-redisearch-m7*
setups), we push the current benchmark's metadata as the snap's
`metadata-external-labels` right before the workload starts, so every
profile sample lands on Polar Signals tagged with the dimensions the
query skills rely on (`test_name`, `git_hash`, `tested_commands`,
`topology`, ...).

Mirrors the mechanism used by the Redis OSS benchmark coordinator:

    sudo snap set parca-agent metadata-external-labels="k1=v1;k2=v2;..."

If parca-agent is not installed / not active, this module is a no-op
(we log once and move on). Failures in this path never abort a
benchmark -- profiling labels are nice-to-have, not load-bearing.
"""
import logging

from redisbench_admin.utils.remote import execute_remote_commands


def _ssh(ip, user, pk, port, cmds, timeout=30):
    """Thin wrapper that swallows SSH exceptions -- this is never fatal."""
    try:
        return execute_remote_commands(ip, user, pk, cmds, port, timeout=timeout)
    except Exception as e:
        logging.warning(
            "parca-agent label plumbing SSH to %s:%s failed: %s", ip, port, e
        )
        return None


def is_parca_agent_running(ip, port, user, pk):
    """Returns True if parca-agent is installed and its snap service is active.

    Uses `snap services parca-agent`; the second line's third column is the
    current state (`active` or `inactive`). If snap isn't present at all,
    the grep fails and we return False.
    """
    probe = (
        "command -v snap >/dev/null 2>&1 "
        "&& snap services parca-agent 2>/dev/null "
        "| awk 'NR==2 {print $3}'"
    )
    res = _ssh(ip, user, pk, port, [probe])
    if not res:
        return False
    exit_code, stdout, _ = res[0]
    if exit_code != 0:
        return False
    state = stdout[0].strip() if stdout else ""
    running = state == "active"
    if running:
        logging.info("parca-agent detected and active on %s -- labels will be injected", ip)
    return running


def _format_labels(labels):
    """Render a {k: v} dict as a parca-agent metadata-external-labels blob.

    Format: `k1=v1;k2=v2;...`. We drop keys with empty / None values and
    sanitize any `;` within a value (replace with `,`) and any `"` (replace
    with `'`) so the quoted shell string stays well-formed.
    """
    parts = []
    for k, v in labels.items():
        if v is None:
            continue
        s = str(v).strip()
        if s == "":
            continue
        s = s.replace(";", ",").replace('"', "'")
        parts.append(f"{k}={s}")
    return ";".join(parts)


def set_parca_agent_labels(ip, port, user, pk, labels):
    """Push metadata-external-labels=... to the parca-agent snap.

    Returns True on successful set. Logs but does not raise on failure.
    """
    blob = _format_labels(labels)
    if not blob:
        logging.info("parca-agent labels: nothing to set (empty label set)")
        return False
    cmd = f'sudo snap set parca-agent metadata-external-labels="{blob}"'
    res = _ssh(ip, user, pk, port, [cmd])
    if not res:
        return False
    exit_code, _, stderr = res[0]
    if exit_code != 0:
        logging.warning(
            "snap set parca-agent metadata-external-labels failed (exit=%d) on %s: %s",
            exit_code,
            ip,
            stderr,
        )
        return False
    preview = blob if len(blob) < 300 else (blob[:297] + "...")
    logging.info("parca-agent labels set on %s: %s", ip, preview)
    return True


def clear_parca_agent_labels(ip, port, user, pk):
    """Clear metadata-external-labels on the snap.

    Call between tests (or on teardown) so a crash mid-run doesn't leave
    stale labels attached to samples from the next test.
    """
    cmd = 'sudo snap set parca-agent metadata-external-labels=""'
    res = _ssh(ip, user, pk, port, [cmd])
    if not res:
        return False
    return res[0][0] == 0


def build_labels(
    setup_name,
    setup_type,
    architecture,
    test_name,
    benchmark_tool,
    metadata_tags,
    tf_github_org,
    tf_github_repo,
    tf_github_branch,
    tf_github_sha,
    tf_triggering_env,
    coordinator_version,
):
    """Compose the metadata-external-labels dict for a single benchmark run.

    Mirrors the label set used by the Redis OSS benchmark coordinator.
    Missing values (e.g. tested_commands for RediSearch specs that don't
    carry that field) are dropped by `_format_labels` rather than recorded
    as empty strings.
    """
    return {
        "platform": setup_name,
        "topology": setup_type,
        "arch": architecture,
        "coordinator_version": coordinator_version,
        "github_org": tf_github_org,
        "github_repo": tf_github_repo,
        "git_branch": tf_github_branch,
        "git_hash": tf_github_sha,
        "triggering_env": tf_triggering_env,
        "test_name": test_name,
        "client_tool": benchmark_tool,
        "tested_commands": (
            metadata_tags.get("command")
            or metadata_tags.get("commands")
            or ""
        ),
        "tested_groups": metadata_tags.get("component", ""),
    }
