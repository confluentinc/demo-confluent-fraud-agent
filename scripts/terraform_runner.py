"""
Terraform execution wrappers: init + apply / destroy in a Terraform root.
"""

import subprocess
import sys
from pathlib import Path


def run_terraform(env_path: Path, auto_approve: bool = True, targets: tuple[str, ...] = ()) -> bool:
    """Run terraform init and apply in env_path. Returns True on success."""
    print(f"\nInitializing Terraform in {env_path}...")
    try:
        subprocess.run(["terraform", "init"], cwd=env_path, check=True)

        apply_cmd = ["terraform", "apply"]
        if auto_approve:
            apply_cmd.append("-auto-approve")
        apply_cmd += [f"-target={target}" for target in targets]

        print(f"Running terraform apply in {env_path}...")
        subprocess.run(apply_cmd, cwd=env_path, check=True)
        print(f"✓ Deployment successful: {env_path.name}")
        return True
    except subprocess.CalledProcessError:
        print(f"✗ Terraform failed in {env_path.name}")
        return False
    except FileNotFoundError:
        print("Error: Terraform not found. Please install Terraform first.")
        sys.exit(1)


def _run_streaming(cmd: list[str], cwd: Path) -> tuple[int, str]:
    """Run a command, echoing output live and returning (returncode, output)."""
    proc = subprocess.Popen(
        cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    )
    lines: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="")
        lines.append(line)
    return proc.wait(), "".join(lines)


def _is_dead_flink_key(output: str) -> bool:
    return "401 Unauthorized" in output and "flink" in output.lower()


def _forget_flink_objects(env_path: Path) -> list[str]:
    """`terraform state rm` every Flink statement/connection; return what was removed."""
    listed = subprocess.run(
        ["terraform", "state", "list"], cwd=env_path, capture_output=True, text=True, check=False
    )
    targets = [
        addr
        for addr in listed.stdout.splitlines()
        if "confluent_flink_statement." in addr or "confluent_flink_connection." in addr
    ]
    if targets:
        subprocess.run(["terraform", "state", "rm", *targets], cwd=env_path, check=True)
    return targets


def run_terraform_destroy(env_path: Path, auto_approve: bool = True) -> bool:
    """Run terraform init and destroy in env_path. Returns True on success."""
    print(f"\nInitializing Terraform in {env_path}...")
    try:
        subprocess.run(["terraform", "init"], cwd=env_path, check=True)

        destroy_cmd = ["terraform", "destroy"]
        if auto_approve:
            destroy_cmd.append("-auto-approve")

        print(f"Running terraform destroy in {env_path}...")
        returncode, output = _run_streaming(destroy_cmd, env_path)

        if returncode != 0 and _is_dead_flink_key(output):
            # The Flink API key in state is already gone (e.g. an earlier partial
            # destroy deleted it), so refreshing/deleting Flink statements and
            # connections 401s forever. Those objects live inside the compute pool
            # and are removed with the environment, so forgetting them is safe.
            dropped = _forget_flink_objects(env_path)
            if dropped:
                print(
                    f"\nFlink API key is no longer valid (401). Removed {len(dropped)} Flink "
                    "statement/connection resource(s) from state — they are deleted with the "
                    "environment. Retrying destroy..."
                )
                returncode, output = _run_streaming(destroy_cmd, env_path)

        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, destroy_cmd)
        print(f"✓ Destroy successful: {env_path.name}")
        return True
    except subprocess.CalledProcessError:
        print(f"✗ Terraform destroy failed in {env_path.name}")
        return False
    except FileNotFoundError:
        print("Error: Terraform not found. Please install Terraform first.")
        sys.exit(1)
