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


def run_terraform_destroy(env_path: Path, auto_approve: bool = True) -> bool:
    """Run terraform init and destroy in env_path. Returns True on success."""
    print(f"\nInitializing Terraform in {env_path}...")
    try:
        subprocess.run(["terraform", "init"], cwd=env_path, check=True)

        destroy_cmd = ["terraform", "destroy"]
        if auto_approve:
            destroy_cmd.append("-auto-approve")

        print(f"Running terraform destroy in {env_path}...")
        subprocess.run(destroy_cmd, cwd=env_path, check=True)
        print(f"✓ Destroy successful: {env_path.name}")
        return True
    except subprocess.CalledProcessError:
        print(f"✗ Terraform destroy failed in {env_path.name}")
        return False
    except FileNotFoundError:
        print("Error: Terraform not found. Please install Terraform first.")
        sys.exit(1)
