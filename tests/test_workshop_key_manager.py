"""Pure-function tests for the workshop AWS key manager (no AWS calls)."""

import logging
from fnmatch import fnmatchcase

from scripts import workshop_key_manager as wkm

ACCOUNT = "123456789012"
PROFILE_ARN = f"arn:aws:bedrock:us-east-1:{ACCOUNT}:inference-profile/us.anthropic.claude-sonnet-4-5-20250929-v1:0"


def _covers(policy: dict, action: str, arn: str) -> bool:
    for s in policy["Statement"]:
        actions = s["Action"] if isinstance(s["Action"], list) else [s["Action"]]
        resources = s["Resource"] if isinstance(s["Resource"], list) else [s["Resource"]]
        if s["Effect"] == "Allow" and action in actions and any(fnmatchcase(arn, r) for r in resources):
            return True
    return False


def test_policy_is_bedrock_invoke_only():
    policy = wkm.get_bedrock_policy(ACCOUNT)
    assert policy["Version"] == "2012-10-17"
    actions = {a for s in policy["Statement"] for a in s["Action"]}
    assert actions == {"bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"}
    assert all(s["Resource"] != "*" for s in policy["Statement"])


def test_policy_matches_terraform_model_id():
    # The inference profile the Flink Bedrock connection calls (terraform/main.tf local.bedrock_model_id).
    assert wkm.BEDROCK_INFERENCE_PROFILE == "us.anthropic.claude-sonnet-4-5-20250929-v1:0"


def test_policy_covers_profile_and_routed_foundation_models():
    policy = wkm.get_bedrock_policy(ACCOUNT)
    assert _covers(policy, "bedrock:InvokeModel", PROFILE_ARN)
    for region in ("us-east-1", "us-east-2", "us-west-2"):
        arn = f"arn:aws:bedrock:{region}::foundation-model/anthropic.claude-sonnet-4-5-20250929-v1:0"
        assert _covers(policy, "bedrock:InvokeModel", arn)
        assert _covers(policy, "bedrock:InvokeModelWithResponseStream", arn)


def test_policy_excludes_other_models_and_regions():
    policy = wkm.get_bedrock_policy(ACCOUNT)
    assert not _covers(
        policy, "bedrock:InvokeModel", "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku"
    )
    assert not _covers(
        policy,
        "bedrock:InvokeModel",
        "arn:aws:bedrock:eu-west-1::foundation-model/anthropic.claude-sonnet-4-5-20250929-v1:0",
    )
    assert not _covers(policy, "bedrock:InvokeModel", PROFILE_ARN.replace(ACCOUNT, "999999999999"))


def test_saved_md_round_trips_username_and_key(tmp_path):
    (tmp_path / "credentials.env").write_text("TF_VAR_owner_email=a@b.c\n")
    tags = wkm.get_tags(tmp_path, "a@b.c")
    wkm.save_aws_credentials_file(
        tmp_path, "AKIATEST", "SECRET", wkm.AWS_REGION, tags, logging.getLogger("t"), username="custom-user"
    )
    assert wkm.parse_iam_username_from_md(tmp_path) == "custom-user"
    assert wkm.parse_access_key_from_md(tmp_path) == "AKIATEST"
    env = (tmp_path / "credentials.env").read_text()
    assert "TF_VAR_aws_bedrock_access_key='AKIATEST'" in env
    assert "TF_VAR_aws_bedrock_secret_key='SECRET'" in env
    assert "tableflow" not in env and "iam_username" not in env


def test_parse_md_missing_file(tmp_path):
    assert wkm.parse_iam_username_from_md(tmp_path) is None
    assert wkm.parse_access_key_from_md(tmp_path) is None
